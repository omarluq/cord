package cord

import (
	"context"
	"errors"
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite" // Register the SQLite driver used by these tests.
)

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
