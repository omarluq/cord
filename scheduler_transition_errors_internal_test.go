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
