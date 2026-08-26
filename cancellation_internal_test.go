package cord

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
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
