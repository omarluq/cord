package cord

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func maintenanceTestStep(_ context.Context, value int) (int, error) {
	return value, nil
}

type heartbeatTestBackend struct {
	storage.Backend
	err       error
	called    chan struct{}
	remaining time.Duration
	accepted  bool
}

func startErrorReporterForTest(t *testing.T, runtime *Cord) {
	t.Helper()

	runtime.ctx, runtime.cancel = context.WithCancel(context.Background())
	runtime.errorReports = make(chan error, schedulerErrorQueueCapacity)

	runtime.errorReporterDone = make(chan struct{})
	go runtime.runErrorReporter()

	t.Cleanup(func() {
		runtime.cancel()

		select {
		case <-runtime.errorReporterDone:
		case <-time.After(time.Second):
			t.Error("scheduler error reporter did not exit")
		}
	})
}

func nonblockingErrorCallback(reports chan<- error) func(error) {
	return func(err error) {
		select {
		case reports <- err:
		default:
		}
	}
}

func newSchedulerCallbackRuntime(t *testing.T, callback func(error)) *Cord {
	t.Helper()

	runtime := newCordWithSettings(nil, "callback-owner", schedulerSettings{
		concurrency: 1, pollInterval: time.Hour, leaseTTL: defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval, retry: defaultRetryPolicy(),
		onSchedulerError: callback,
	})

	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	return runtime
}

const (
	completeTransition = "complete"
	failTransition     = "fail"
	retryTransition    = "retry"
	raceRunID          = "race-run"
	raceNodeID         = "race-node"
	raceOwner          = "race-owner"
)

type rejectedTransitionBackend struct {
	storage.Backend
	resultErr      error
	transitionErr  error
	transition     string
	terminalReason storage.TerminalReason
	result         storage.RunResult
}

func (backend *rejectedTransitionBackend) CompleteNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	storage.EncodedPayload,
) (bool, error) {
	backend.transition = completeTransition

	return false, backend.transitionErr
}

func (backend *rejectedTransitionBackend) FailNode(
	_ context.Context,
	_ storage.RunID,
	_ storage.NodeID,
	_ storage.Lease,
	_ storage.EncodedPayload,
	reason storage.TerminalReason,
) (bool, error) {
	backend.transition = failTransition
	backend.terminalReason = reason

	return false, backend.transitionErr
}

func (backend *rejectedTransitionBackend) RetryNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	storage.EncodedPayload,
	time.Duration,
) (bool, error) {
	backend.transition = retryTransition

	return false, backend.transitionErr
}

func (backend *rejectedTransitionBackend) GetRunResult(
	context.Context,
	storage.RunID,
) (storage.RunResult, error) {
	return backend.result, backend.resultErr
}

func TestCord_TerminalFailureReason(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		err        error
		name       string
		wantReason storage.TerminalReason
		attempt    int
	}{
		{
			name: "permanent before final attempt", err: Permanent(errors.New("permanent")), attempt: 1,
			wantReason: storage.ReasonFailureNonRetryable,
		},
		{
			name: "retryable final attempt", err: errors.New("exhausted"), attempt: 3,
			wantReason: storage.ReasonFailureAttemptsExhausted,
		},
		{
			name: "permanent final attempt", err: Permanent(errors.New("permanent and exhausted")), attempt: 3,
			wantReason: storage.ReasonFailureNonRetryable,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: storage.RunResult{
				WorkflowName: "", DefinitionHash: "", TerminalSignatureHash: "",
				Status: storage.RunFailed, Output: nil, Error: nil,
				MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
			}}
			runtime := &Cord{store: backend}
			claim := &storage.Claim{
				RunID: raceRunID, NodeID: raceNodeID, FunctionKey: "", SignatureHash: "",
				Lease: storage.Lease{}, Attempt: testCase.attempt, MaxAttempts: 3,
				RetryBaseDelay: time.Second, RetryMaxDelay: time.Second, RetryPolicyVersion: 0,
			}

			require.Error(t, runtime.handleFailure(t.Context(), claim, testCase.err))
			require.Equal(t, failTransition, backend.transition)
			require.Equal(t, testCase.wantReason, backend.terminalReason)
		})
	}
}

func (backend heartbeatTestBackend) HeartbeatNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	time.Duration,
) (bool, time.Duration, error) {
	select {
	case backend.called <- struct{}{}:
	default:
	}

	return backend.accepted, backend.remaining, backend.err
}

func TestCord_RejectedFencedTransitionsAreClassified(t *testing.T) {
	t.Parallel()

	const staleOwner = "stale"

	claim := &storage.Claim{
		RunID: raceRunID, NodeID: raceNodeID, FunctionKey: "", SignatureHash: "",
		Lease:   storage.Lease{ExpiresAt: time.Time{}, Owner: staleOwner, Generation: 1, Remaining: 0},
		Attempt: 1, MaxAttempts: 3, RetryBaseDelay: time.Second, RetryMaxDelay: time.Second,
		RetryPolicyVersion: 0,
	}

	testCases := []struct {
		run         func(*Cord) error
		name        string
		transition  string
		status      storage.RunStatus
		wantMessage string
		wantClass   fencedTransitionClass
	}{
		{
			name: "completion versus expiry", transition: completeTransition, status: storage.RunRunning,
			wantClass: fencedTransitionLeaseLost, wantMessage: "lease ownership was lost",
			run: func(runtime *Cord) error {
				return runtime.completeClaim(t.Context(), claim, []byte(`"stale"`))
			},
		},
		{
			name: "failure versus cancellation", transition: failTransition, status: storage.RunCanceled,
			wantClass: fencedTransitionCancellationWon, wantMessage: "run cancellation already won",
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, Permanent(errors.New("stale failure")))
			},
		},
		{
			name: "retry versus recovery", transition: retryTransition, status: storage.RunRunning,
			wantClass: fencedTransitionLeaseLost, wantMessage: "lease ownership was lost",
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, errors.New("stale retry"))
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: *testRunResult(testCase.status, nil)}
			runtime := &Cord{store: backend}

			err := testCase.run(runtime)

			var rejected *fencedTransitionError
			require.ErrorAs(t, err, &rejected)
			require.Equal(t, testCase.wantClass, rejected.class)
			require.ErrorContains(t, err, testCase.wantMessage)
			require.Equal(t, testCase.transition, backend.transition)
		})
	}
}

func TestCord_ExpectedRejectedClaimTransitionsAreNotReported(t *testing.T) {
	t.Parallel()

	claim := &storage.Claim{
		RunID: raceRunID, NodeID: raceNodeID, FunctionKey: "", SignatureHash: "",
		Lease: storage.Lease{
			ExpiresAt: time.Time{}, Owner: raceOwner, Generation: 1, Remaining: 0,
		},
		Attempt: 1, MaxAttempts: 3, RetryBaseDelay: time.Second, RetryMaxDelay: time.Second,
		RetryPolicyVersion: 0,
	}

	testCases := []struct {
		run        func(*Cord) error
		name       string
		transition string
		status     storage.RunStatus
	}{
		{
			name: "cancellation versus completion", transition: completeTransition, status: storage.RunCanceled,
			run: func(runtime *Cord) error {
				return runtime.completeClaim(t.Context(), claim, []byte(`"result"`))
			},
		},
		{
			name: "cancellation versus failure", transition: failTransition, status: storage.RunCanceled,
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, Permanent(errors.New("failure")))
			},
		},
		{
			name: "cancellation versus retry", transition: retryTransition, status: storage.RunCanceling,
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, errors.New("retry"))
			},
		},
		{
			name: "durable completion versus completion", transition: completeTransition, status: storage.RunCompleted,
			run: func(runtime *Cord) error {
				return runtime.completeClaim(t.Context(), claim, []byte(`"result"`))
			},
		},
		{
			name: "durable failure versus retry", transition: retryTransition, status: storage.RunFailed,
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, errors.New("retry"))
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: *testRunResult(testCase.status, nil)}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			runtime := &Cord{
				store: backend, ctx: ctx, errorReports: make(chan error, 1), onSchedulerError: func(error) {},
			}

			runtime.reportClaimTransitionError(testCase.run(runtime))

			require.Equal(t, testCase.transition, backend.transition)
			require.Empty(t, runtime.errorReports)
		})
	}
}

func TestCord_ExpectedRejectedClaimReleaseIsNotReported(t *testing.T) {
	t.Parallel()

	for _, status := range []storage.RunStatus{
		storage.RunCanceling, storage.RunCanceled, storage.RunCompleted, storage.RunFailed,
	} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: *testRunResult(status, nil)}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			runtime := &Cord{
				store: backend, ctx: ctx, errorReports: make(chan error, 1), onSchedulerError: func(error) {},
			}

			runtime.releaseClaim(&storage.Claim{
				RunID: raceRunID, NodeID: raceNodeID, FunctionKey: "", SignatureHash: "",
				Lease: storage.Lease{
					ExpiresAt: time.Time{}, Owner: raceOwner, Generation: 1, Remaining: 0,
				},
				Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0,
				RetryPolicyVersion: 0,
			}, errors.New("registration disappeared"))

			require.Equal(t, retryTransition, backend.transition)
			require.Empty(t, runtime.errorReports)
		})
	}
}

func TestCord_RejectedClaimReleaseAfterReclaimIsReported(t *testing.T) {
	t.Parallel()

	reports := make(chan error, 1)
	backend := &rejectedTransitionBackend{result: *testRunResult(storage.RunRunning, nil)}
	runtime := &Cord{
		store: backend, onSchedulerError: func(err error) { reports <- err },
	}
	startErrorReporterForTest(t, runtime)

	claim := &storage.Claim{
		RunID: "reclaimed-run", NodeID: "reclaimed-node", FunctionKey: "", SignatureHash: "",
		Lease:   storage.Lease{ExpiresAt: time.Time{}, Owner: "stale", Generation: 1, Remaining: 0},
		Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
	}

	runtime.releaseClaim(claim, errors.New("registration disappeared"))

	report := <-reports

	var rejected *fencedTransitionError
	require.ErrorAs(t, report, &rejected)
	require.Equal(t, fencedTransitionLeaseLost, rejected.class)
	require.ErrorContains(t, report, "claim release rejected: lease ownership was lost")
	require.Equal(t, retryTransition, backend.transition)
}

func TestCord_RejectedFencedTransitionPreservesDurableWinner(t *testing.T) {
	t.Parallel()

	winner := storage.EncodedPayload(`"winner"`)
	backend := &rejectedTransitionBackend{result: *testRunResult(storage.RunCompleted, winner)}
	runtime := &Cord{store: backend}
	claim := &storage.Claim{
		RunID: "completed-run", NodeID: "completed-node", FunctionKey: "", SignatureHash: "",
		Lease:   storage.Lease{ExpiresAt: time.Time{}, Owner: "stale", Generation: 1, Remaining: 0},
		Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
	}

	err := runtime.completeClaim(t.Context(), claim, []byte(`"stale"`))

	var rejected *fencedTransitionError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, fencedTransitionDurableWinner, rejected.class)
	require.ErrorContains(t, err, `durable run outcome "completed" already won`)
	require.Equal(t, winner, backend.result.Output)
}

func testClaim(runID storage.RunID, nodeID storage.NodeID) *storage.Claim {
	return &storage.Claim{
		RunID: runID, NodeID: nodeID, FunctionKey: "", SignatureHash: "",
		Lease: storage.Lease{
			ExpiresAt: time.Time{}, Owner: raceOwner, Generation: 1, Remaining: 0,
		},
		Attempt: 1, MaxAttempts: 3, RetryBaseDelay: time.Second, RetryMaxDelay: time.Second,
		RetryPolicyVersion: 1,
	}
}

func TestCord_ClaimTransitionStorageFailureIsReported(t *testing.T) {
	t.Parallel()

	transitionErr := errors.New("complete unavailable")
	backend := &rejectedTransitionBackend{transitionErr: transitionErr}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	runtime := &Cord{
		store: backend, ctx: ctx, errorReports: make(chan error, 1), onSchedulerError: func(error) {},
	}
	claim := testClaim("storage-run", "storage-node")

	runtime.reportClaimTransitionError(runtime.completeClaim(t.Context(), claim, nil))

	report := <-runtime.errorReports
	require.ErrorIs(t, report, transitionErr)
	require.ErrorContains(t, report, "complete node")
}

func TestCord_RejectedClaimTransitionImpossibleStateIsReported(t *testing.T) {
	t.Parallel()

	backend := &rejectedTransitionBackend{result: *testRunResult(storage.RunStatus("invalid"), nil)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	runtime := &Cord{
		store: backend, ctx: ctx, errorReports: make(chan error, 1), onSchedulerError: func(error) {},
	}
	claim := testClaim("invalid-run", "invalid-node")

	runtime.reportClaimTransitionError(runtime.completeClaim(t.Context(), claim, nil))

	report := <-runtime.errorReports

	var rejected *fencedTransitionError
	require.ErrorAs(t, report, &rejected)
	require.Equal(t, fencedTransitionImpossibleState, rejected.class)
}

func TestCord_RejectedFencedTransitionClassificationFailureIsReported(t *testing.T) {
	t.Parallel()

	classifyErr := errors.New("result unavailable")
	backend := &rejectedTransitionBackend{resultErr: classifyErr}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	runtime := &Cord{
		store: backend, ctx: ctx, errorReports: make(chan error, 1), onSchedulerError: func(error) {},
	}
	claim := &storage.Claim{
		RunID: "unknown-run", NodeID: "unknown-node", FunctionKey: "", SignatureHash: "",
		Lease: storage.Lease{}, Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0,
		RetryMaxDelay: 0, RetryPolicyVersion: 0,
	}

	runtime.reportClaimTransitionError(runtime.completeClaim(t.Context(), claim, nil))

	report := <-runtime.errorReports
	require.ErrorIs(t, report, classifyErr)
	require.ErrorContains(t, report, "classify rejected node completion")
}

func TestCord_HeartbeatPermitExhaustionIsReportedOnce(t *testing.T) {
	t.Parallel()

	runtime := &Cord{
		ctx:              t.Context(),
		heartbeatCalls:   make(chan struct{}, 1),
		errorReports:     make(chan error, 2),
		onSchedulerError: func(error) {},
	}
	runtime.heartbeatCalls <- struct{}{}

	state := &heartbeatState{}

	runtime.startHeartbeatCall(t.Context(), nil, state)
	runtime.startHeartbeatCall(t.Context(), nil, state)

	select {
	case report := <-runtime.errorReports:
		require.ErrorContains(t, report, "heartbeat call capacity exhausted")
	default:
		t.Fatal("heartbeat permit exhaustion was not reported")
	}

	require.Empty(t, runtime.errorReports)
}

func TestCord_HeartbeatFailureCancelsBeforeLeaseExpiry(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		heartbeatErr error
		name         string
		wantReport   bool
	}{
		{name: "lease rejected"},
		{name: "storage errors persist", heartbeatErr: errors.New("heartbeat unavailable"), wantReport: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			called := make(chan struct{}, 1)
			reports := make(chan error, 1)
			backend := heartbeatTestBackend{accepted: false, err: testCase.heartbeatErr, called: called}
			runtime := &Cord{
				store: backend, heartbeatInterval: 50 * time.Millisecond, leaseTTL: 200 * time.Millisecond,
				onSchedulerError: nonblockingErrorCallback(reports),
			}
			startErrorReporterForTest(t, runtime)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan bool, 1)
			claim := &storage.Claim{
				RunID: "", NodeID: "", FunctionKey: "", SignatureHash: "",
				Lease: storage.Lease{Owner: "", Generation: 0, Remaining: 200 * time.Millisecond,
					ExpiresAt: time.Now().Add(time.Hour)},
				Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
			}

			go runtime.heartbeat(ctx, claim, cancel, done)

			<-called

			select {
			case held := <-done:
				require.False(t, held)
			case <-time.After(time.Second):
				t.Fatal("heartbeat did not stop before the test deadline")
			}

			if testCase.wantReport {
				select {
				case report := <-reports:
					require.ErrorContains(t, report, testCase.heartbeatErr.Error())
				case <-time.After(time.Second):
					t.Fatal("heartbeat error was not reported")
				}
			} else {
				require.Empty(t, reports)
			}
		})
	}
}

func TestCord_HeartbeatUsesDatabaseRelativeLifetimeRegardlessOfWallClock(t *testing.T) {
	t.Parallel()

	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		t.Run(skew.String(), func(t *testing.T) {
			t.Parallel()

			called := make(chan struct{}, 1)
			backend := heartbeatTestBackend{
				accepted: true, remaining: 200 * time.Millisecond, called: called,
			}
			runtime := &Cord{
				store: backend, heartbeatInterval: 20 * time.Millisecond, leaseTTL: 200 * time.Millisecond,
			}
			ctx, stop := context.WithCancel(t.Context())
			done := make(chan bool, 1)
			claim := &storage.Claim{
				RunID: "", NodeID: "", FunctionKey: "", SignatureHash: "",
				Lease: storage.Lease{
					ExpiresAt: time.Now().Add(skew), Owner: "", Generation: 0, Remaining: 80 * time.Millisecond,
				},
				Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
			}

			go runtime.heartbeat(ctx, claim, stop, done)

			select {
			case <-called:
			case <-time.After(time.Second):
				t.Fatal("heartbeat followed wall-clock expiry instead of relative lifetime")
			}

			stop()
			require.True(t, <-done)
		})
	}
}

func TestCord_InvokeClaimDiscardsClaimsForFinishedRuns(t *testing.T) {
	t.Parallel()

	for _, status := range []storage.RunStatus{
		storage.RunCompleted,
		storage.RunFailed,
		storage.RunCanceling,
		storage.RunCanceled,
	} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			backend := &claimAdmissionBackend{result: *testRunResult(status, nil)}
			runtime := &Cord{store: backend}
			executionCtx, cancel := context.WithCancel(t.Context())
			heartbeatDone := make(chan bool, 1)

			heartbeatDone <- true

			invoked := false

			output, leaseHeld, err := runtime.invokeClaim(
				executionCtx,
				testClaim("finished-run", "stale-node"),
				registeredInvocation{invoke: func(
					context.Context,
					[]storage.EncodedPayload,
				) (storage.EncodedPayload, error) {
					invoked = true

					return nil, nil
				}},
				cancel,
				heartbeatDone,
			)

			require.NoError(t, err)
			require.Nil(t, output)
			require.False(t, leaseHeld)
			require.False(t, invoked)
			require.Zero(t, backend.inputLoads)
			require.ErrorIs(t, executionCtx.Err(), context.Canceled)
			require.Empty(t, runtime.activeAttempts)
		})
	}
}

type claimAdmissionBackend struct {
	storage.Backend
	resultErr  error
	result     storage.RunResult
	inputLoads int
	retries    int
}

func (backend *claimAdmissionBackend) GetRunResult(
	context.Context,
	storage.RunID,
) (storage.RunResult, error) {
	return backend.result, backend.resultErr
}

func (backend *claimAdmissionBackend) LoadNodeInputs(
	context.Context,
	storage.RunID,
	storage.NodeID,
) ([]storage.EncodedPayload, error) {
	backend.inputLoads++

	return nil, nil
}

func (backend *claimAdmissionBackend) RetryNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	storage.EncodedPayload,
	time.Duration,
) (bool, error) {
	backend.retries++

	return true, nil
}

func TestCord_ClaimAdmissionReportsInvalidAndStorageStates(t *testing.T) {
	t.Parallel()

	storageErr := errors.New("result unavailable")
	testCases := []struct {
		name       string
		status     storage.RunStatus
		storageErr error
		want       string
	}{
		{name: "invalid status", status: storage.RunStatus("invalid"), want: `invalid durable run status "invalid"`},
		{name: "storage failure", status: storage.RunRunning, storageErr: storageErr, want: storageErr.Error()},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &claimAdmissionBackend{
				result:    *testRunResult(testCase.status, nil),
				resultErr: testCase.storageErr,
			}
			runtime := &Cord{
				store:            backend,
				ctx:              context.Background(),
				errorReports:     make(chan error, 1),
				onSchedulerError: func(error) {},
			}
			executionCtx, cancel := context.WithCancel(t.Context())
			heartbeatDone := make(chan bool, 1)

			heartbeatDone <- true

			output, leaseHeld, err := runtime.invokeClaim(
				executionCtx,
				testClaim("invalid-run", "stale-node"),
				registeredInvocation{},
				cancel,
				heartbeatDone,
			)

			require.NoError(t, err)
			require.Nil(t, output)
			require.False(t, leaseHeld)
			require.ErrorContains(t, <-runtime.errorReports, testCase.want)
			require.ErrorIs(t, executionCtx.Err(), context.Canceled)
			require.Empty(t, runtime.activeAttempts)
			require.Zero(t, backend.inputLoads)
			require.Equal(t, 1, backend.retries)
		})
	}
}

func TestCord_ActiveAttemptRegistryUnregistersWithoutLeaks(t *testing.T) {
	t.Parallel()

	runtime := &Cord{activeAttempts: make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)}
	claim := testClaim("registry-run", "registry-node")
	ctx, cancel := context.WithCancel(t.Context())

	runtime.activeMu.Lock()
	unregister := runtime.registerActiveAttemptLocked(claim, cancel)
	runtime.activeMu.Unlock()
	require.Len(t, runtime.activeAttempts[claim.RunID], 1)

	unregister()
	require.NotContains(t, runtime.activeAttempts, claim.RunID)

	unregister()
	require.NotContains(t, runtime.activeAttempts, claim.RunID)
	cancel()
	<-ctx.Done()
}

func TestCord_NotifyCompletionCancelsAllActiveAttempts(t *testing.T) {
	t.Parallel()

	runtime := &Cord{
		activeAttempts:    make(map[storage.RunID]map[activeAttemptKey]*activeAttempt),
		completionWaiters: make(map[storage.RunID]*completionPoll),
	}
	claim := testClaim("canceled-run", "active-node")
	contexts := make([]context.Context, 0, 2)

	runtime.activeMu.Lock()
	for range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		contexts = append(contexts, ctx)

		runtime.registerActiveAttemptLocked(claim, cancel)
	}
	runtime.activeMu.Unlock()

	runtime.notifyCompletion(claim.RunID)

	for _, ctx := range contexts {
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	}

	require.NotContains(t, runtime.activeAttempts, claim.RunID)
}

func TestCord_ActiveAttemptCancellationRacesUnregistration(t *testing.T) {
	t.Parallel()

	for range 100 {
		runtime := &Cord{activeAttempts: make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)}
		claim := testClaim("race-registry-run", "race-registry-node")
		ctx, cancel := context.WithCancel(t.Context())

		runtime.activeMu.Lock()
		unregister := runtime.registerActiveAttemptLocked(claim, cancel)
		runtime.activeMu.Unlock()

		var wait sync.WaitGroup

		wait.Add(2)

		go func() {
			defer wait.Done()

			runtime.cancelActiveAttempts(claim.RunID)
		}()
		go func() {
			defer wait.Done()

			unregister()
		}()

		wait.Wait()
		cancel()
		<-ctx.Done()
		require.NotContains(t, runtime.activeAttempts, claim.RunID)
	}
}

func TestCord_ActiveAttemptKeysSeparateClaims(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		change func(*storage.Claim)
		name   string
	}{
		{name: "generation", change: func(claim *storage.Claim) { claim.Lease.Generation++ }},
		{name: "node", change: func(claim *storage.Claim) { claim.NodeID = "other-node" }},
		{name: "run", change: func(claim *storage.Claim) { claim.RunID = "other-run" }},
		{name: "lease owner", change: func(claim *storage.Claim) { claim.Lease.Owner = "other-owner" }},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runtime := &Cord{activeAttempts: make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)}
			firstClaim := testClaim("key-run", "key-node")
			secondClaim := testClaim("key-run", "key-node")
			testCase.change(secondClaim)

			firstCtx, firstCancel := context.WithCancel(t.Context())
			secondCtx, secondCancel := context.WithCancel(t.Context())
			t.Cleanup(firstCancel)
			t.Cleanup(secondCancel)

			runtime.activeMu.Lock()
			unregisterFirst := runtime.registerActiveAttemptLocked(firstClaim, firstCancel)
			runtime.registerActiveAttemptLocked(secondClaim, secondCancel)
			runtime.activeMu.Unlock()

			unregisterFirst()
			unregisterFirst()
			runtime.cancelActiveAttempts(secondClaim.RunID)

			require.NoError(t, firstCtx.Err())
			require.ErrorIs(t, secondCtx.Err(), context.Canceled)
		})
	}
}

func TestCord_ActiveAttemptReplacementCannotBeRemovedByOldUnregister(t *testing.T) {
	t.Parallel()

	runtime := &Cord{activeAttempts: make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)}
	claim := testClaim("replacement-run", "replacement-node")
	oldCtx, oldCancel := context.WithCancel(t.Context())
	newCtx, newCancel := context.WithCancel(t.Context())

	runtime.activeMu.Lock()
	unregisterOld := runtime.registerActiveAttemptLocked(claim, oldCancel)
	runtime.registerActiveAttemptLocked(claim, newCancel)
	runtime.activeMu.Unlock()

	require.ErrorIs(t, oldCtx.Err(), context.Canceled)
	unregisterOld()
	runtime.cancelActiveAttempts(claim.RunID)
	require.ErrorIs(t, newCtx.Err(), context.Canceled)
	newCancel()
}

func TestCord_HeartbeatAcceptedBoundaryIsConservative(t *testing.T) {
	t.Parallel()

	called := make(chan struct{}, 1)
	backend := heartbeatTestBackend{accepted: true, remaining: 20 * time.Millisecond, called: called}
	runtime := &Cord{store: backend, heartbeatInterval: 20 * time.Millisecond, leaseTTL: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan bool, 1)
	claim := &storage.Claim{
		RunID: "", NodeID: "", FunctionKey: "", SignatureHash: "",
		Lease: storage.Lease{
			ExpiresAt: time.Time{}, Owner: "", Generation: 0, Remaining: 100 * time.Millisecond,
		},
		Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
	}

	go runtime.heartbeat(ctx, claim, cancel, done)

	<-called
	require.False(t, <-done)
}

func TestCord_SchedulerErrorCallbackPanicIsRecovered(t *testing.T) {
	t.Parallel()

	callbackContinued := make(chan struct{})

	var calls atomic.Int32

	runtime := newSchedulerCallbackRuntime(t, func(error) {
		if calls.Add(1) == 1 {
			panic("callback panic")
		}

		close(callbackContinued)
	})

	runtime.reportSchedulerError(errors.New("panic"))
	runtime.reportSchedulerError(errors.New("continue"))

	select {
	case <-callbackContinued:
	case <-time.After(time.Second):
		t.Fatal("callback reporter did not continue after panic")
	}
}

func TestCord_SchedulerErrorCallbackMayCallLifecycleMethods(t *testing.T) {
	t.Parallel()

	testCases := map[string]func(*Cord) error{
		"Close": func(runtime *Cord) error { return runtime.Close() },
		"Shutdown": func(runtime *Cord) error {
			return runtime.Shutdown(context.Background())
		},
	}

	for name, lifecycle := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			result := make(chan error, 1)

			var runtime *Cord

			runtime = newSchedulerCallbackRuntime(t, func(error) {
				result <- lifecycle(runtime)
			})

			runtime.reportSchedulerError(errors.New("lifecycle callback"))

			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("lifecycle call deadlocked in scheduler error callback")
			}
		})
	}
}

func TestCord_SchedulerErrorBurstUsesBoundedOverflowSummary(t *testing.T) {
	t.Parallel()

	callbackStarted := make(chan struct{})
	callbackRelease := make(chan struct{})
	reports := make(chan error, schedulerErrorQueueCapacity+2)
	runtime := newSchedulerCallbackRuntime(t, func(err error) {
		select {
		case <-callbackStarted:
		default:
			close(callbackStarted)
			<-callbackRelease
		}

		reports <- err
	})

	first := errors.New("first")
	runtime.reportSchedulerError(first)
	<-callbackStarted

	const overflow = 7
	for index := range schedulerErrorQueueCapacity + overflow {
		runtime.reportSchedulerError(fmt.Errorf("burst error %d", index))
	}

	close(callbackRelease)

	require.ErrorIs(t, <-reports, first)
	summary := <-reports

	var dropped schedulerErrorsDroppedError
	require.ErrorAs(t, summary, &dropped)
	require.Equal(t, uint64(overflow), dropped.count)

	for range schedulerErrorQueueCapacity {
		select {
		case report := <-reports:
			require.NotErrorAs(t, report, &dropped)
		case <-time.After(time.Second):
			t.Fatal("queued scheduler error was not reported")
		}
	}
}

func TestCord_SchedulerErrorCallbackDoesNotBlockReporting(t *testing.T) {
	t.Parallel()

	callbackStarted := make(chan struct{}, 1)
	callbackRelease := make(chan struct{})

	var callbackCalls atomic.Int32

	runtime := newSchedulerCallbackRuntime(t, func(error) {
		callbackCalls.Add(1)

		select {
		case callbackStarted <- struct{}{}:
		default:
		}

		<-callbackRelease
	})

	runtime.reportSchedulerError(context.Canceled)
	<-callbackStarted

	reported := make(chan struct{})

	go func() {
		runtime.reportSchedulerError(context.DeadlineExceeded)
		close(reported)
	}()

	<-reported

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Close() }()

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close waited for a blocked scheduler error callback")
	}

	close(callbackRelease)

	select {
	case <-runtime.errorReporterDone:
	case <-time.After(time.Second):
		t.Fatal("released scheduler error reporter did not exit after shutdown")
	}

	require.Equal(t, int32(1), callbackCalls.Load(), "shutdown must discard queued reports")
}

func TestCord_SchedulerErrorReportingRacesWithShutdown(t *testing.T) {
	t.Parallel()

	callbackStarted := make(chan struct{})
	callbackRelease := make(chan struct{})
	runtime := newSchedulerCallbackRuntime(t, func(error) {
		select {
		case <-callbackStarted:
		default:
			close(callbackStarted)
		}

		<-callbackRelease
	})

	runtime.reportSchedulerError(errors.New("block callback"))
	<-callbackStarted

	start := make(chan struct{})

	var reporters sync.WaitGroup
	for reporter := range 32 {
		reporters.Go(func() {
			<-start

			for report := range 100 {
				runtime.reportSchedulerError(fmt.Errorf("reporter %d error %d", reporter, report))
			}
		})
	}

	shutdownDone := make(chan error, 1)

	go func() {
		<-start

		shutdownDone <- runtime.Close()
	}()

	close(start)

	reporters.Wait()

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("concurrent reporting prevented shutdown")
	}

	close(callbackRelease)

	select {
	case <-runtime.errorReporterDone:
	case <-time.After(time.Second):
		t.Fatal("scheduler error reporter did not exit after concurrent shutdown")
	}
}

func TestCord_WakeDoesNotRunMaintenance(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "cord.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	store, err := sqlite.New(database)
	require.NoError(t, err)

	schedulerErrors := make(chan error, 1)
	runtime := newCordWithSettings(store, "test-owner", schedulerSettings{
		concurrency:       1,
		pollInterval:      time.Hour,
		leaseTTL:          defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval,
		retry:             defaultRetryPolicy(),
		onSchedulerError:  func(err error) { schedulerErrors <- err },
	})

	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	workflow := runtime.From("maintenance-wake", maintenanceTestStep)
	nodes, err := workflow.graph.compile(workflow.tail)
	require.NoError(t, err)
	plan, err := buildPlan(workflow.graph.name, nodes, workflow.tail, 1, runtime.retry)
	require.NoError(t, err)
	require.NoError(t, store.CreateRun(t.Context(), plan))
	_, err = database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET status = 'retry_wait', available_at = datetime('now', '-1 second')
		WHERE run_id = ?`, plan.Run.ID)
	require.NoError(t, err)

	_, err = database.ExecContext(t.Context(), `CREATE TRIGGER reject_scheduler_maintenance
		BEFORE UPDATE ON cord_nodes BEGIN SELECT RAISE(FAIL, 'maintenance ran'); END`)
	require.NoError(t, err)

	runtime.signalScheduler()
	require.Eventually(t, func() bool { return len(runtime.wake) == 0 }, time.Second, time.Millisecond)

	select {
	case err := <-schedulerErrors:
		t.Fatalf("unexpected scheduler error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

type admissionTestBackend struct {
	storage.Backend
	createPanic    any
	createStarted  chan struct{}
	allowCreate    chan struct{}
	created        chan storage.RunID
	resultRead     chan struct{}
	result         resultStore
	startOnce      sync.Once
	resultReadOnce sync.Once
	attached       bool
}

func (backend *admissionTestBackend) CreateRun(ctx context.Context, plan *storage.RunPlan) error {
	_, _, err := backend.CreateOrAttachRun(ctx, plan)

	return err
}

func (backend *admissionTestBackend) CreateOrAttachRun(
	ctx context.Context,
	plan *storage.RunPlan,
) (storage.RunID, bool, error) {
	backend.startOnce.Do(func() { close(backend.createStarted) })

	if backend.createPanic != nil {
		panic(backend.createPanic)
	}

	select {
	case <-backend.allowCreate:
	case <-ctx.Done():
		return "", false, fmt.Errorf("admission test create run: %w", ctx.Err())
	}

	backend.created <- plan.Run.ID

	return plan.Run.ID, !backend.attached, nil
}

func (backend *admissionTestBackend) GetRunResult(context.Context, storage.RunID) (storage.RunResult, error) {
	result := backend.result.get()

	if backend.resultRead != nil {
		backend.resultReadOnce.Do(func() { close(backend.resultRead) })
	}

	return result, nil
}

func (*admissionTestBackend) ClaimReadyNodeForFunctions(
	context.Context,
	string,
	time.Duration,
	[]storage.FunctionRegistration,
) (*storage.Claim, bool, error) {
	return nil, false, nil
}

func newAdmissionTestRuntime(backend storage.Backend) *Cord {
	return newCordWithSettings(backend, "admission-test", schedulerSettings{
		concurrency:       1,
		pollInterval:      time.Hour,
		leaseTTL:          defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval,
		retry:             defaultRetryPolicy(),
	})
}

func admissionTestStep(_ context.Context, input int) (int, error) { return input + 1, nil }

func TestWorkflowPersistRunWakesSchedulerAfterAttach(t *testing.T) {
	t.Parallel()

	allowCreate := make(chan struct{})
	close(allowCreate)

	backend := &admissionTestBackend{
		attached: true, createStarted: make(chan struct{}), allowCreate: allowCreate,
		created: make(chan storage.RunID, 1),
	}
	runtime := &Cord{
		store: backend, wake: make(chan struct{}, 1), admittedRuns: 1,
		acceptingRuns: true, admissionMu: sync.Mutex{},
	}
	workflow := Workflow[int, int]{runtime: runtime}
	plan := &storage.RunPlan{
		Nodes: nil, Edges: nil,
		Run: storage.Run{
			CreatedAt: time.Time{}, UpdatedAt: time.Time{}, CompletedAt: nil, StartedAt: nil,
			TerminalReason: nil, TerminalRunnerID: nil,
			ID: "attached-run", WorkflowName: "", DefinitionHash: "",
			IdempotencyKey: nil, SubmissionFingerprint: nil, TerminalNodeID: "",
			Status: "", Input: nil, Output: nil, Error: nil,
			MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
		},
	}

	id, _, err := workflow.persistRun(t.Context(), plan)
	require.NoError(t, err)
	require.Equal(t, storage.RunID("attached-run"), id)
	require.Len(t, runtime.wake, 1)
}

// TestWorkflowRunCreateRunPanicReleasesAdmission verifies that a CreateRun
// panic releases admission and allows shutdown to complete.
func TestWorkflowRunCreateRunPanicReleasesAdmission(t *testing.T) {
	t.Parallel()

	const panicValue = "create run panic"

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		createPanic:   panicValue,
	}
	runtime := newAdmissionTestRuntime(backend)
	flow := runtime.From("panic-during-create", admissionTestStep)

	panicResult := make(chan any, 1)

	go func() {
		defer func() { panicResult <- recover() }()

		if _, err := flow.Run(t.Context(), 1); err != nil {
			panic(err)
		}
	}()

	select {
	case recovered := <-panicResult:
		require.Equal(t, panicValue, recovered)
	case <-time.After(time.Second):
		t.Fatal("CreateRun panic did not propagate")
	}

	shutdownCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	require.NoError(t, runtime.Shutdown(shutdownCtx))
}

func TestWorkflowRunRejectsSubmissionAfterShutdownBegins(t *testing.T) {
	t.Parallel()

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		allowCreate:   make(chan struct{}),
		created:       make(chan storage.RunID, 1),
	}
	runtime := newAdmissionTestRuntime(backend)
	require.NoError(t, runtime.Close())

	_, err := runtime.From("shutdown-before-submit", admissionTestStep).Run(t.Context(), 1)
	require.EqualError(t, err, "cord: runtime closed")

	select {
	case <-backend.createStarted:
		t.Fatal("submission reached persistence after shutdown began")
	default:
	}
}

func TestWorkflowRunShutdownDuringSubmissionRejectsBeforePersistence(t *testing.T) {
	t.Parallel()

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		allowCreate:   make(chan struct{}),
		created:       make(chan storage.RunID, 1),
	}
	runtime := newAdmissionTestRuntime(backend)
	flow := runtime.From("shutdown-during-submit", admissionTestStep)

	// Hold the admission boundary after validation and plan construction are
	// available, then linearize shutdown before allowing Run to request admission.
	runtime.admissionMu.Lock()
	runDone := make(chan error, 1)

	go func() {
		_, err := flow.Run(t.Context(), 1)
		runDone <- err
	}()

	// Apply beginShutdown's admission transition while the test owns the
	// boundary, making shutdown the deterministic winner.
	runtime.acceptingRuns = false
	runtime.admissionMu.Unlock()

	require.EqualError(t, <-runDone, "cord: runtime closed")
	require.NoError(t, runtime.Close())

	select {
	case <-backend.createStarted:
		t.Fatal("submission persisted after losing admission")
	default:
	}
}

// TestWorkflowRunPersistenceWinningShutdownRaceRemainsReported verifies that a
// persisted running submission remains observable when shutdown wins afterward.
func TestWorkflowRunPersistenceWinningShutdownRaceRemainsReported(t *testing.T) {
	t.Parallel()

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		allowCreate:   make(chan struct{}),
		created:       make(chan storage.RunID, 1),
		resultRead:    make(chan struct{}),
		result:        newResultStore(testRunResult(storage.RunRunning, nil)),
	}
	runtime := newAdmissionTestRuntime(backend)
	flow := runtime.From("persist-versus-close", admissionTestStep)

	runDone := make(chan struct {
		err    error
		result int
	}, 1)
	go func() {
		result, err := flow.Run(t.Context(), 1)
		runDone <- struct {
			err    error
			result int
		}{err: err, result: result}
	}()

	<-backend.createStarted // Persistence proves submission already won admission.

	shutdownCtx, cancel := context.WithCancel(t.Context())
	cancel()

	shutdownErr := runtime.Shutdown(shutdownCtx)
	require.True(t, shutdownErr == nil || errors.Is(shutdownErr, context.Canceled))

	close(backend.allowCreate)
	createdID := <-backend.created
	require.NotEmpty(t, createdID)
	<-runtime.ctx.Done()
	<-backend.resultRead

	backend.result.set(testRunResult(storage.RunCompleted, storage.EncodedPayload("2")))
	runtime.notifyCompletion(createdID)

	outcome := <-runDone
	require.NoError(t, outcome.err)
	require.Equal(t, 2, outcome.result)
	require.NoError(t, runtime.Close())
}
