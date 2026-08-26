package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStore_CompleteNodeReleasesDependenciesAndCompletesTerminalRun(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "complete")
	requireCreateRun(t.Context(), t, store, &plan)

	first := claimNode(t, store)
	accepted, err := store.CompleteNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"object"`))
	require.NoError(t, err)
	require.True(t, accepted)
	assertNodeState(t, database, first.RunID, first.NodeID, storage.NodeCompleted, 0)
	assertNodeState(t, database, first.RunID, "publish", storage.NodeReady, 0)
	assertRunState(t, database, first.RunID, storage.RunRunning, nil)

	terminal := claimNode(t, store)
	accepted, err = store.CompleteNode(
		t.Context(), terminal.RunID, terminal.NodeID, terminal.Lease, []byte(`"released"`),
	)
	require.NoError(t, err)
	require.True(t, accepted)
	assertRunState(t, database, terminal.RunID, storage.RunCompleted, []byte(`"released"`))

	accepted, err = store.CompleteNode(
		t.Context(), terminal.RunID, terminal.NodeID, terminal.Lease, []byte(`"overwrite"`),
	)
	require.NoError(t, err)
	assert.False(t, accepted)
	assertRunState(t, database, terminal.RunID, storage.RunCompleted, []byte(`"released"`))
}

func TestStore_CompleteNodeQueuesReleasedChildrenBehindWaitingWork(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	now := time.Now().UTC()
	active := validPlan(now.Add(-2*time.Minute), "active-run")
	waiting := validPlan(now.Add(-time.Minute), "waiting-run")

	requireCreateRun(t.Context(), t, store, &active)
	requireCreateRun(t.Context(), t, store, &waiting)

	first := claimNode(t, store)
	require.Equal(t, active.Run.ID, first.RunID)

	accepted, err := store.CompleteNode(
		t.Context(),
		first.RunID,
		first.NodeID,
		first.Lease,
		[]byte(`"compiled"`),
	)
	require.NoError(t, err)
	require.True(t, accepted)

	next := claimNode(t, store)
	assert.Equal(t, waiting.Run.ID, next.RunID)
	assert.Equal(t, compileNode, next.NodeID)

	var releasedLater bool
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		`SELECT julianday(released.available_at) > julianday(waiting.available_at)
		FROM cord_nodes AS released, cord_nodes AS waiting
		WHERE released.run_id = ? AND released.node_id = ?
			AND waiting.run_id = ? AND waiting.node_id = ?`,
		active.Run.ID,
		active.Run.TerminalNodeID,
		waiting.Run.ID,
		compileNode,
	).Scan(&releasedLater))
	assert.True(t, releasedLater)
}

func TestStore_CompleteNodeDoesNotStarveReadyBranch(t *testing.T) {
	t.Parallel()

	const joinNode storage.NodeID = "join"

	_, store := newStore(t, true)
	now := time.Now().UTC()
	plan := validPlan(now.Add(-2*time.Minute), "branch-fairness")
	plan.Nodes = append(plan.Nodes,
		newNode(
			plan.Run.ID,
			"waiting-branch",
			"example.com/workflow.Waiting",
			"waiting-signature",
			storage.NodeReady,
			now.Add(-time.Minute),
			0,
		),
		newNode(
			plan.Run.ID,
			joinNode,
			"example.com/workflow.Join",
			"join-signature",
			storage.NodePending,
			now,
			2,
		),
	)
	plan.Edges = append(plan.Edges,
		storage.Edge{RunID: plan.Run.ID, Parent: terminalNode, Child: joinNode, ParentOrder: 0},
		storage.Edge{RunID: plan.Run.ID, Parent: "waiting-branch", Child: joinNode, ParentOrder: 1},
	)
	plan.Run.TerminalNodeID = joinNode
	requireCreateRun(t.Context(), t, store, &plan)

	first := claimNode(t, store)
	require.Equal(t, compileNode, first.NodeID)
	accepted, err := store.CompleteNode(
		t.Context(),
		first.RunID,
		first.NodeID,
		first.Lease,
		[]byte(`"compiled"`),
	)
	require.NoError(t, err)
	require.True(t, accepted)

	next := claimNode(t, store)
	assert.Equal(t, storage.NodeID("waiting-branch"), next.NodeID)
}

func TestStore_CompleteNodeRejectsStaleAndExpiredLeases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*storage.Lease)
		name   string
	}{
		{name: "owner", mutate: func(lease *storage.Lease) { lease.Owner = "stale" }},
		{name: "generation", mutate: func(lease *storage.Lease) { lease.Generation-- }},
		{name: "expired in database", mutate: nil},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("fence-"+testCase.name))
			requireCreateRun(t.Context(), t, store, &plan)
			claim := claimNode(t, store)

			if testCase.mutate == nil {
				_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
					SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
					WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID)
				require.NoError(t, err)
			} else {
				testCase.mutate(&claim.Lease)
			}

			accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("bad"))
			require.NoError(t, err)
			assert.False(t, accepted)
			assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)
			assertNodeState(t, database, claim.RunID, "publish", storage.NodePending, 1)
		})
	}
}

func TestStore_CompleteNodeRollsBackAtDependencyReleaseBoundary(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "completion-rollback")
	requireCreateRun(t.Context(), t, store, &plan)
	claim := claimNode(t, store)

	_, err := database.ExecContext(t.Context(), `CREATE TRIGGER reject_ready BEFORE UPDATE OF status ON cord_nodes
		WHEN NEW.status = 'ready' BEGIN SELECT RAISE(ABORT, 'crash boundary'); END`)
	require.NoError(t, err)

	accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("must-rollback"))
	require.ErrorContains(t, err, "crash boundary")
	assert.False(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)
	assertNodeState(t, database, claim.RunID, "publish", storage.NodePending, 1)
	assertRunState(t, database, claim.RunID, storage.RunRunning, nil)
}
