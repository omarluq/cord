package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStore_FailNodeIsFencedAndAtomic(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "failure")
	requireCreateRun(t.Context(), t, store, &plan)
	claim := claimNode(t, store)
	stale := claim.Lease
	stale.Generation--

	accepted, err := store.FailNode(
		t.Context(), claim.RunID, claim.NodeID, stale, []byte("stale"),
		storage.ReasonFailureNonRetryable,
	)
	require.NoError(t, err)
	assert.False(t, accepted)
	assertRunState(t, database, claim.RunID, storage.RunRunning, nil)

	accepted, err = store.FailNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("permanent"),
		storage.ReasonFailureNonRetryable,
	)
	require.NoError(t, err)
	require.True(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeFailed, 0)
	assertNodeReason(t, database, claim.RunID, claim.NodeID, storage.ReasonFailureNonRetryable)
	assertNodeState(t, database, claim.RunID, "publish", storage.NodeCanceled, 1)
	assertRunFailure(t, database, claim.RunID, []byte("permanent"))
	assertRunReason(t, database, claim.RunID, storage.ReasonFailureNonRetryable)
}

func TestStore_CancelRunFencesRunningWorkAndPreservesCompletedNodes(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "cancel")
	requireCreateRun(t.Context(), t, store, &plan)
	claim := claimNode(t, store)

	accepted, err := sqlite.CancelRunForTest(t.Context(), store, claim.RunID)
	require.NoError(t, err)
	require.True(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeCanceled, 0)
	assertNodeState(t, database, claim.RunID, "publish", storage.NodeCanceled, 1)
	assertRunState(t, database, claim.RunID, storage.RunCanceled, nil)

	completed, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("late"))
	require.NoError(t, err)
	assert.False(t, completed)
	accepted, err = sqlite.CancelRunForTest(t.Context(), store, claim.RunID)
	require.NoError(t, err)
	assert.False(t, accepted)
}
