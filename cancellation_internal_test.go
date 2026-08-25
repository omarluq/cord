package cord

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite" // Register the SQLite driver used by these tests.
)

type cancelBackendResult struct {
	err     error
	outcome storage.CancellationOutcome
}

type cancellationBackend struct {
	storage.Backend
	cancelResults []cancelBackendResult
	results       []storage.RunResult
	resultErrs    []error
	resultErr     error
	result        storage.RunResult
	cancelCalls   int
	resultCalls   int
	mu            sync.Mutex
}

func (backend *cancellationBackend) CancelRun(
	context.Context,
	storage.RunID,
) (storage.CancellationOutcome, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	index := min(backend.cancelCalls, len(backend.cancelResults)-1)
	backend.cancelCalls++

	return backend.cancelResults[index].outcome, backend.cancelResults[index].err
}

func (*cancellationBackend) ClaimReadyNodeForFunctions(
	context.Context,
	string,
	time.Duration,
	[]storage.FunctionRegistration,
) (*storage.Claim, bool, error) {
	return nil, false, nil
}

func (*cancellationBackend) PromoteRetries(context.Context) (int64, error) {
	return 0, nil
}

func (*cancellationBackend) RecoverExpiredLeases(context.Context) (int64, error) {
	return 0, nil
}

func (backend *cancellationBackend) GetRunResult(
	context.Context,
	storage.RunID,
) (storage.RunResult, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	index := backend.resultCalls
	backend.resultCalls++

	if len(backend.results) > 0 {
		resultIndex := min(index, len(backend.results)-1)

		var err error
		if len(backend.resultErrs) > 0 {
			err = backend.resultErrs[min(index, len(backend.resultErrs)-1)]
		}

		return backend.results[resultIndex], err
	}

	return backend.result, backend.resultErr
}

func (backend *cancellationBackend) calls() (cancel, result int) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	return backend.cancelCalls, backend.resultCalls
}

func cancellationRunResult(status storage.RunStatus) storage.RunResult {
	return *testRunResult(status, nil)
}

func TestWorkflowCancelRequiresOnlyRuntimeAndRunID(t *testing.T) {
	t.Parallel()

	graphErr := errors.New("unrelated graph error")
	backend := &cancellationBackend{cancelResults: []cancelBackendResult{{
		outcome: storage.CancellationCanceled,
	}}}
	flow := Workflow[int, int]{runtime: cancellationRuntime(backend), err: graphErr}

	require.NoError(t, flow.Cancel(t.Context(), "retained-run"))

	cancelCalls, resultCalls := backend.calls()
	assert.Equal(t, 1, cancelCalls)
	assert.Zero(t, resultCalls)
}

func cancellationShutdownStep(_ context.Context, input int) (int, error) {
	return input, nil
}

func TestWorkflowCancelAfterRuntimeShutdown(t *testing.T) {
	t.Parallel()

	backend := &cancellationBackend{cancelResults: []cancelBackendResult{{
		outcome: storage.CancellationCanceled,
	}}}
	runtime := newCordWithSettings(backend, "cancellation-shutdown-test", schedulerSettings{
		concurrency: 1, pollInterval: time.Hour, leaseTTL: defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval, retry: defaultRetryPolicy(),
	})
	flow := runtime.From("cancel-after-runtime-shutdown", cancellationShutdownStep)
	require.NoError(t, runtime.Close())

	require.NoError(t, flow.Cancel(t.Context(), "retained-run"))

	cancelCalls, resultCalls := backend.calls()
	assert.Equal(t, 1, cancelCalls)
	assert.Zero(t, resultCalls)
}

func TestWorkflowCancelValidatesBeforeStorage(t *testing.T) {
	t.Parallel()

	backend := &cancellationBackend{cancelResults: []cancelBackendResult{{
		outcome: storage.CancellationCanceled,
	}}}
	runtime := cancellationRuntime(backend)

	var nilRuntime Workflow[int, int]
	require.ErrorContains(t, nilRuntime.Cancel(t.Context(), "run"), "invalid workflow")
	require.ErrorContains(t, (Workflow[int, int]{runtime: runtime}).Cancel(t.Context(), ""), "run ID is empty")

	cancelCalls, resultCalls := backend.calls()
	assert.Zero(t, cancelCalls)
	assert.Zero(t, resultCalls)
}

func TestWorkflowCancelReconcilesAmbiguousOutcome(t *testing.T) {
	t.Parallel()

	operationErr := errors.New("commit acknowledgement lost")
	readErr := errors.New("read unavailable")
	tests := []struct {
		resultErr  error
		want       error
		wantJoined error
		name       string
		status     storage.RunStatus
	}{
		{name: "canceled", status: storage.RunCanceled},
		{name: "completed", status: storage.RunCompleted, want: ErrRunFinished},
		{name: "failed", status: storage.RunFailed, want: ErrRunFinished},
		{name: "missing", resultErr: storage.ErrRunNotFound, want: ErrRunNotFound},
		{name: "running", status: storage.RunRunning, want: operationErr},
		{
			name: "read failure", status: storage.RunRunning,
			resultErr: readErr, want: operationErr,
			wantJoined: readErr,
		},
		{
			name: "incompatible persisted run", status: storage.RunRunning,
			resultErr: storage.ErrRunIncompatible, want: operationErr,
			wantJoined: ErrRunIncompatible,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &cancellationBackend{
				cancelResults: []cancelBackendResult{{err: operationErr}},
				result:        cancellationRunResult(testCase.status),
				resultErr:     testCase.resultErr,
			}
			flow := Workflow[int, int]{runtime: cancellationRuntime(backend)}

			err := flow.Cancel(t.Context(), "run")
			if testCase.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, testCase.want)
			}

			if testCase.wantJoined != nil {
				require.ErrorIs(t, err, testCase.wantJoined)
			}

			cancelCalls, resultCalls := backend.calls()
			assert.Equal(t, 1, cancelCalls)
			assert.Equal(t, 1, resultCalls)
		})
	}
}

func TestWorkflowCancelReconciledCancellationNotifiesLocalAttempt(t *testing.T) {
	t.Parallel()

	backend := &cancellationBackend{
		cancelResults: []cancelBackendResult{{err: errors.New("response lost")}},
		result:        cancellationRunResult(storage.RunCanceled),
	}
	runtime := cancellationRuntime(backend)
	claim := testClaim("run", "node")
	attemptCtx, cancel := context.WithCancel(t.Context())

	runtime.activeMu.Lock()
	runtime.registerActiveAttemptLocked(claim, cancel)
	runtime.activeMu.Unlock()

	flow := Workflow[int, int]{runtime: runtime}
	require.NoError(t, flow.Cancel(t.Context(), "run"))
	require.ErrorIs(t, attemptCtx.Err(), context.Canceled)
}

func TestWorkflowCancelPreservesCanceledCallerAndOperationErrors(t *testing.T) {
	t.Parallel()

	operationErr := errors.New("ambiguous cancellation")
	backend := &cancellationBackend{cancelResults: []cancelBackendResult{{err: operationErr}}}
	flow := Workflow[int, int]{runtime: cancellationRuntime(backend)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := flow.Cancel(ctx, "run")
	require.ErrorIs(t, err, operationErr)
	require.ErrorIs(t, err, context.Canceled)

	_, resultCalls := backend.calls()
	assert.Zero(t, resultCalls)
}

func TestWorkflowCancelRetriesObservedPendingCancellationOnce(t *testing.T) {
	t.Parallel()

	operationErr := errors.New("first response lost")
	tests := []struct {
		retry  cancelBackendResult
		want   error
		name   string
		result storage.RunResult
	}{
		{
			name:  "retry finishes cancellation",
			retry: cancelBackendResult{outcome: storage.CancellationAlreadyCanceled},
		},
		{
			name:  "retry returns unknown outcome",
			retry: cancelBackendResult{outcome: storage.CancellationOutcome("future")},
			want:  operationErr,
		},
		{
			name:   "retry error reconciles canceled",
			retry:  cancelBackendResult{err: errors.New("second response lost")},
			result: cancellationRunResult(storage.RunCanceled),
		},
		{
			name:   "retry error remains pending",
			retry:  cancelBackendResult{err: errors.New("second response lost")},
			result: cancellationRunResult(storage.RunCanceling),
			want:   operationErr,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &cancellationBackend{
				cancelResults: []cancelBackendResult{{err: operationErr}, testCase.retry},
				results: []storage.RunResult{
					cancellationRunResult(storage.RunCanceling),
					testCase.result,
				},
			}

			flow := Workflow[int, int]{runtime: cancellationRuntime(backend)}

			err := flow.Cancel(t.Context(), "run")
			if testCase.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, testCase.want)
			}

			// A failed retry must remain discoverable alongside the first ambiguous error.
			if testCase.retry.err != nil && err != nil {
				require.ErrorIs(t, err, testCase.retry.err)
			}

			if testCase.retry.outcome == storage.CancellationOutcome("future") {
				require.ErrorContains(t, err, `unknown run cancellation outcome "future"`)
			}

			cancelCalls, _ := backend.calls()
			assert.Equal(t, 2, cancelCalls)
		})
	}
}

func TestWorkflowCancelRejectsUnknownReconciledStatus(t *testing.T) {
	t.Parallel()

	operationErr := errors.New("ambiguous cancellation")
	backend := &cancellationBackend{
		cancelResults: []cancelBackendResult{{err: operationErr}},
		result:        cancellationRunResult(storage.RunStatus("future")),
	}
	flow := Workflow[int, int]{runtime: cancellationRuntime(backend)}

	err := flow.Cancel(t.Context(), "run")
	require.ErrorIs(t, err, operationErr)
	require.ErrorContains(t, err, `unknown durable run status "future"`)
}

func cancellationEndToEndStep(ctx context.Context, directory string) (string, error) {
	if err := os.WriteFile(filepath.Join(directory, "started"), []byte("started"), 0o600); err != nil {
		return "", fmt.Errorf("write cancellation start marker: %w", err)
	}

	<-ctx.Done()
	cancellationErr := ctx.Err()

	if err := os.WriteFile(filepath.Join(directory, "canceled"), []byte("canceled"), 0o600); err != nil {
		return "", errors.Join(cancellationErr, fmt.Errorf("write cancellation marker: %w", err))
	}

	return directory, fmt.Errorf("canceled test attempt: %w", cancellationErr)
}

type acknowledgedCancellationErrorBackend struct {
	storage.Backend
	err error
}

func (backend *acknowledgedCancellationErrorBackend) CancelRun(
	ctx context.Context,
	runID storage.RunID,
) (storage.CancellationOutcome, error) {
	outcome, err := backend.Backend.CancelRun(ctx, runID)
	if err != nil {
		return outcome, fmt.Errorf("delegate cancellation: %w", err)
	}

	return outcome, backend.err
}

func TestWorkflowCancelReconcilesCommittedCancellationEndToEnd(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "cancel.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	require.NoError(t, sqlite.Migrate(t.Context(), database))

	store, err := sqlite.New(database)
	require.NoError(t, err)

	ackErr := errors.New("cancellation acknowledgement lost")
	backend := &acknowledgedCancellationErrorBackend{Backend: store, err: ackErr}
	runtime := newCordWithSettings(backend, "cancellation-test", schedulerSettings{
		concurrency: 1, pollInterval: time.Hour, leaseTTL: defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval, retry: defaultRetryPolicy(),
	})

	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	directory := t.TempDir()
	flow := runtime.From("cancel-acknowledgement-error", cancellationEndToEndStep)
	runID, err := flow.Submit(t.Context(), directory)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(directory, "started"))

		return statErr == nil
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, flow.Cancel(t.Context(), runID))
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(directory, "canceled"))

		return statErr == nil
	}, 5*time.Second, time.Millisecond)
	_, err = flow.Get(t.Context(), runID)
	require.ErrorIs(t, err, ErrRunCanceled)

	report, err := runtime.InspectRun(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, RunStateCanceled, report.State)
	assert.Equal(t, ReasonCanceledByRequest, report.Reason)
}

func cancellationRuntime(backend storage.Backend) *Cord {
	return &Cord{
		store:             backend,
		completionWaiters: make(map[storage.RunID]*completionPoll),
		activeAttempts:    make(map[storage.RunID]map[activeAttemptKey]*activeAttempt),
	}
}
