package sqlite_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_CancelRunCancelsEveryUnfinishedStateAndPreservesTerminalNodes(t *testing.T) {
	t.Parallel()

	const waitingNode = "waiting"

	database, store := newStore(t, true)
	now := time.Now().UTC()
	plan := validPlan(now, "cancel-states")
	plan.Nodes = []storage.Node{
		newNode(
			plan.Run.ID,
			"completed",
			"completed",
			"completed-signature",
			storage.NodeReady,
			now.Add(-4*time.Second),
			0,
		),
		newNode(plan.Run.ID, "active", "active", "active-signature", storage.NodeReady, now.Add(-3*time.Second), 0),
		newNode(plan.Run.ID, "ready", "ready", "ready-signature", storage.NodeReady, now.Add(-2*time.Second), 0),
		newNode(plan.Run.ID, waitingNode, waitingNode, "waiting-signature", storage.NodePending, now, 2),
		newNode(plan.Run.ID, "retry", "retry", "retry-signature", storage.NodeReady, now.Add(-time.Second), 0),
	}
	plan.Edges = []storage.Edge{
		{RunID: plan.Run.ID, Parent: "completed", Child: waitingNode, ParentOrder: 0},
		{RunID: plan.Run.ID, Parent: "active", Child: waitingNode, ParentOrder: 1},
	}
	plan.Run.TerminalNodeID = waitingNode
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	completeClaim := claimNode(t, store)
	require.Equal(t, storage.NodeID("completed"), completeClaim.NodeID)
	accepted, err := store.CompleteNode(
		t.Context(),
		completeClaim.RunID,
		completeClaim.NodeID,
		completeClaim.Lease,
		[]byte("done"),
	)
	require.NoError(t, err)
	require.True(t, accepted)

	runningClaim := claimNode(t, store)
	require.Equal(t, storage.NodeID("active"), runningClaim.NodeID)
	retryClaim := claimNode(t, store)
	require.Equal(t, storage.NodeID("ready"), retryClaim.NodeID)
	accepted, err = store.RetryNode(
		t.Context(),
		retryClaim.RunID,
		retryClaim.NodeID,
		retryClaim.Lease,
		[]byte("retry"),
		time.Hour,
	)
	require.NoError(t, err)
	require.True(t, accepted)

	accepted, err = store.CancelRun(t.Context(), plan.Run.ID)
	require.NoError(t, err)
	require.True(t, accepted)

	assertNodeState(t, database, plan.Run.ID, "completed", storage.NodeCompleted, 0)
	assertNodeState(t, database, plan.Run.ID, "active", storage.NodeCanceled, 0)
	assertNodeState(t, database, plan.Run.ID, "ready", storage.NodeCanceled, 0)
	assertNodeState(t, database, plan.Run.ID, waitingNode, storage.NodeCanceled, 1)
	assertNodeState(t, database, plan.Run.ID, "retry", storage.NodeCanceled, 0)
	assertRunState(t, database, plan.Run.ID, storage.RunCanceled, nil)
}

func TestStore_CancelRunRollsBackWhenNodeCancellationFails(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "cancel-rollback")
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	_, err := database.ExecContext(t.Context(), `CREATE TRIGGER reject_canceled BEFORE UPDATE OF status ON cord_nodes
		WHEN NEW.status = 'canceled' BEGIN SELECT RAISE(ABORT, 'cancel boundary'); END`)
	require.NoError(t, err)

	accepted, err := store.CancelRun(t.Context(), plan.Run.ID)
	require.ErrorContains(t, err, "cancel boundary")
	assert.False(t, accepted)
	assertRunState(t, database, plan.Run.ID, storage.RunRunning, nil)
	assertNodeState(t, database, plan.Run.ID, plan.Nodes[0].ID, storage.NodeReady, 0)
	assertNodeState(t, database, plan.Run.ID, plan.Nodes[1].ID, storage.NodePending, 1)
}

func TestStore_CancelRunRollsBackWhenFinalRunUpdateFails(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "cancel-final-rollback")
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)

	_, err := database.ExecContext(t.Context(), `CREATE TRIGGER reject_canceled_run BEFORE UPDATE OF status ON cord_runs
		WHEN NEW.status = 'canceled' BEGIN SELECT RAISE(ABORT, 'final cancellation boundary'); END`)
	require.NoError(t, err)

	accepted, err := store.CancelRun(t.Context(), plan.Run.ID)
	require.ErrorContains(t, err, "final cancellation boundary")
	assert.False(t, accepted)
	assertRunState(t, database, plan.Run.ID, storage.RunRunning, nil)
	assertNodeState(t, database, plan.Run.ID, claim.NodeID, storage.NodeRunning, 0)
	assertNodeState(t, database, plan.Run.ID, plan.Nodes[1].ID, storage.NodePending, 1)
}
