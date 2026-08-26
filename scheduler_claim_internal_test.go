package cord

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

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
