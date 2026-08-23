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

type rejectedTransitionBackend struct {
	storage.Backend
	resultErr  error
	transition string
	result     storage.RunResult
}

func (backend *rejectedTransitionBackend) CompleteNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	storage.EncodedPayload,
) (bool, error) {
	backend.transition = "complete"

	return false, nil
}

func (backend *rejectedTransitionBackend) FailNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	storage.EncodedPayload,
) (bool, error) {
	backend.transition = "fail"

	return false, nil
}

func (backend *rejectedTransitionBackend) RetryNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	storage.EncodedPayload,
	time.Duration,
) (bool, error) {
	backend.transition = "retry"

	return false, nil
}

func (backend *rejectedTransitionBackend) GetRunResult(
	context.Context,
	storage.RunID,
) (storage.RunResult, error) {
	return backend.result, backend.resultErr
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
		RunID: "race-run", NodeID: "race-node", FunctionKey: "", SignatureHash: "",
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
			name: "completion versus expiry", transition: "complete", status: storage.RunRunning,
			wantClass: fencedTransitionLeaseLost, wantMessage: "lease ownership was lost",
			run: func(runtime *Cord) error {
				return runtime.completeClaim(t.Context(), claim, []byte(`"stale"`))
			},
		},
		{
			name: "failure versus cancellation", transition: "fail", status: storage.RunCanceled,
			wantClass: fencedTransitionCancellationWon, wantMessage: "run cancellation already won",
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, Permanent(errors.New("stale failure")))
			},
		},
		{
			name: "retry versus recovery", transition: "retry", status: storage.RunRunning,
			wantClass: fencedTransitionLeaseLost, wantMessage: "lease ownership was lost",
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, errors.New("stale retry"))
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: storage.RunResult{
				Status: testCase.status, Output: nil, Error: nil,
			}}
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

func TestCord_RejectedClaimReleaseAfterReclaimIsReported(t *testing.T) {
	t.Parallel()

	reports := make(chan error, 1)
	backend := &rejectedTransitionBackend{result: storage.RunResult{
		Status: storage.RunRunning, Output: nil, Error: nil,
	}}
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
	require.Equal(t, "retry", backend.transition)
}

func TestCord_RejectedFencedTransitionPreservesDurableWinner(t *testing.T) {
	t.Parallel()

	winner := storage.EncodedPayload(`"winner"`)
	backend := &rejectedTransitionBackend{result: storage.RunResult{
		Status: storage.RunCompleted,
		Output: winner,
		Error:  nil,
	}}
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

func TestCord_RejectedFencedTransitionClassificationFailureIsReported(t *testing.T) {
	t.Parallel()

	classifyErr := errors.New("result unavailable")
	backend := &rejectedTransitionBackend{resultErr: classifyErr}
	runtime := &Cord{store: backend}
	claim := &storage.Claim{
		RunID: "unknown-run", NodeID: "unknown-node", FunctionKey: "", SignatureHash: "",
		Lease: storage.Lease{}, Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0,
		RetryMaxDelay: 0, RetryPolicyVersion: 0,
	}

	err := runtime.completeClaim(t.Context(), claim, nil)

	require.ErrorIs(t, err, classifyErr)
	require.ErrorContains(t, err, "classify rejected node completion")
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

	createStarted chan struct{}
	allowCreate   chan struct{}
	created       chan storage.RunID
	result        storage.RunResult

	startOnce sync.Once
}

func (backend *admissionTestBackend) CreateRun(ctx context.Context, plan *storage.RunPlan) error {
	backend.startOnce.Do(func() { close(backend.createStarted) })

	select {
	case <-backend.allowCreate:
	case <-ctx.Done():
		return fmt.Errorf("admission test create run: %w", ctx.Err())
	}

	backend.created <- plan.Run.ID

	return nil
}

func (backend *admissionTestBackend) GetRunResult(context.Context, storage.RunID) (storage.RunResult, error) {
	return backend.result, nil
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

func TestWorkflowRunPersistenceWinningShutdownRaceRemainsReported(t *testing.T) {
	t.Parallel()

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		allowCreate:   make(chan struct{}),
		created:       make(chan storage.RunID, 1),
		result: storage.RunResult{
			Status: storage.RunCompleted,
			Output: storage.EncodedPayload("2"),
			Error:  nil,
		},
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

	outcome := <-runDone
	require.NoError(t, outcome.err)
	require.Equal(t, 2, outcome.result)
	require.NoError(t, runtime.Close())
}
