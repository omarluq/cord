package sqlite_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_InspectRunSnapshotAndCounts(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	now := time.Date(2025, time.January, 2, 3, 4, 5, 6000000, time.FixedZone("test", 2*60*60))
	plan := validPlan(now, "inspect-legacy")
	requireCreateRun(t.Context(), t, store, &plan)

	report, err := store.InspectRun(t.Context(), plan.Run.ID)
	require.NoError(t, err)
	assert.Equal(t, plan.Run.ID, report.ID)
	assert.Equal(t, storage.RunRunning, report.State)
	assert.Empty(t, report.Reason)
	assert.Equal(t, time.UTC, report.SubmittedAt.Location())
	assert.Equal(t, time.UTC, report.StateChangedAt.Location())
	assert.Equal(t, storage.NodeStateCounts{
		Pending: 1, Ready: 1, Running: 0, RetryWait: 0, Completed: 0, Failed: 0, Canceled: 0,
	}, report.NodeCounts)

	var input, output []byte
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT input_payload, output_payload FROM cord_runs WHERE id = ?", plan.Run.ID,
	).Scan(&input, &output))
	assert.Equal(t, []byte(plan.Run.Input), input)
	assert.Nil(t, output)
}

func TestStore_InspectRunLegacyMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status storage.RunStatus
		reason storage.TerminalReason
	}{
		{status: storage.RunCompleted, reason: storage.ReasonSucceeded},
		{status: storage.RunFailed, reason: storage.ReasonLegacyUnknown},
		{status: storage.RunCanceled, reason: storage.ReasonCanceledByRequest},
	}

	for _, testCase := range tests {
		t.Run(string(testCase.status), func(t *testing.T) {
			t.Parallel()
			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("legacy-"+testCase.status))
			requireCreateRun(t.Context(), t, store, &plan)
			_, err := database.ExecContext(t.Context(), `UPDATE cord_runs
				SET status = ?, completed_at = updated_at, lifecycle_version = NULL,
					started_at = NULL, terminal_reason = NULL, terminal_runner_id = NULL
				WHERE id = ?`, testCase.status, plan.Run.ID)
			require.NoError(t, err)

			report, err := store.InspectRun(t.Context(), plan.Run.ID)
			require.NoError(t, err)
			assert.Equal(t, testCase.reason, report.Reason)
		})
	}
}

func TestStore_InspectRunFailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*testing.T, *sql.DB, storage.RunID)
		name   string
	}{
		{
			name: "unknown version",
			mutate: func(t *testing.T, database *sql.DB, runID storage.RunID) {
				t.Helper()
				_, err := database.ExecContext(t.Context(),
					"UPDATE cord_runs SET lifecycle_version = 2 WHERE id = ?", runID)
				require.NoError(t, err)
			},
		},
		{
			name: "missing terminal reason",
			mutate: func(t *testing.T, database *sql.DB, runID storage.RunID) {
				t.Helper()
				_, err := database.ExecContext(t.Context(), `UPDATE cord_runs
					SET lifecycle_version = 1, status = 'failed', completed_at = updated_at
					WHERE id = ?`, runID)
				require.NoError(t, err)
			},
		},
		{
			name: "unknown state",
			mutate: func(t *testing.T, database *sql.DB, runID storage.RunID) {
				t.Helper()
				_, err := database.ExecContext(t.Context(),
					"UPDATE cord_runs SET status = 'future' WHERE id = ?", runID)
				require.NoError(t, err)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("bad-"+testCase.name))
			requireCreateRun(t.Context(), t, store, &plan)
			testCase.mutate(t, database, plan.Run.ID)

			_, err := store.InspectRun(t.Context(), plan.Run.ID)
			assert.ErrorIs(t, err, storage.ErrRunIncompatible)
		})
	}

	_, store := newStore(t, true)
	_, err := store.InspectRun(t.Context(), "missing")
	assert.ErrorIs(t, err, storage.ErrRunNotFound)
}

func TestStore_ListRunNodesKeysetFiltersAndBounds(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	plan := inspectionLinearPlan("node-page", 4)
	requireCreateRun(t.Context(), t, store, &plan)

	page, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: "", PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 2)
	assert.Equal(t, storage.NodeID("node-00"), page.Nodes[0].NodeID)
	assert.Equal(t, "node-01", page.ContinuationToken)

	next, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: page.ContinuationToken, PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, next.Nodes, 2)
	assert.Equal(t, storage.NodeID("node-02"), next.Nodes[0].NodeID)
	assert.Empty(t, next.ContinuationToken)

	state := storage.NodePending
	filtered, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: &state, Reason: nil, ContinuationToken: "", PageSize: 0,
	})
	require.NoError(t, err)
	require.Len(t, filtered.Nodes, 3)

	for _, node := range filtered.Nodes {
		assert.Equal(t, storage.NodePending, node.State)
	}

	_, err = store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: "", PageSize: storage.MaxNodePageSize + 1,
	})
	require.Error(t, err)
	_, err = store.ListRunNodes(t.Context(), "missing", storage.NodeQuery{})
	assert.ErrorIs(t, err, storage.ErrRunNotFound)
}

func TestStore_ListRunNodesLegacyReasonFilterAndMalformedRow(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := inspectionLinearPlan("reason-page", 2)
	requireCreateRun(t.Context(), t, store, &plan)
	_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET status = 'completed', completed_at = available_at, lifecycle_version = NULL,
			state_changed_at = NULL
		WHERE run_id = ? AND node_id = ?`,
		plan.Run.ID, plan.Nodes[0].ID)
	require.NoError(t, err)

	reason := storage.ReasonSucceeded
	page, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: nil, Reason: &reason, ContinuationToken: "", PageSize: 0,
	})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 1)
	assert.Equal(t, reason, page.Nodes[0].Reason)

	_, err = database.ExecContext(t.Context(),
		"UPDATE cord_nodes SET lifecycle_version = 2 WHERE run_id = ?", plan.Run.ID)
	require.NoError(t, err)
	_, err = store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
	assert.ErrorIs(t, err, storage.ErrRunIncompatible)
}

func TestStore_InspectionQueriesAreReadOnly(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "read-only-inspection")
	requireCreateRun(t.Context(), t, store, &plan)
	_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET status = ?, available_at = '2000-01-01T00:00:00Z'
		WHERE run_id = ? AND node_id = ?`, storage.NodeRetryWait, plan.Run.ID, plan.Nodes[0].ID)
	require.NoError(t, err)

	_, err = store.InspectRun(t.Context(), plan.Run.ID)
	require.NoError(t, err)
	_, err = store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
	require.NoError(t, err)

	var status storage.NodeStatus
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT status FROM cord_nodes WHERE run_id = ? AND node_id = ?", plan.Run.ID, plan.Nodes[0].ID,
	).Scan(&status))
	assert.Equal(t, storage.NodeRetryWait, status)
}

func TestStore_ListRunNodesRejectsInvalidFilters(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	unknown := storage.NodeStatus("unknown")
	_, err := store.ListRunNodes(t.Context(), "run", storage.NodeQuery{
		State: &unknown, Reason: nil, ContinuationToken: "", PageSize: 0,
	})
	require.Error(t, err)

	state := storage.NodeReady
	reason := storage.ReasonSucceeded
	_, err = store.ListRunNodes(t.Context(), "run", storage.NodeQuery{
		State: &state, Reason: &reason, ContinuationToken: "", PageSize: 0,
	})
	require.Error(t, err)
}

func inspectionLinearPlan(runID storage.RunID, count int) storage.RunPlan {
	now := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	plan := validPlan(now, runID)
	plan.Nodes = make([]storage.Node, count)

	plan.Edges = make([]storage.Edge, 0, count-1)
	for index := range count {
		nodeID := storage.NodeID(fmt.Sprintf("node-%02d", index))
		status := storage.NodePending
		remaining := 1

		if index == 0 {
			status = storage.NodeReady
			remaining = 0
		}

		plan.Nodes[index] = newNode(runID, nodeID, "inspect.Step", "signature", status, now, remaining)
		if index > 0 {
			plan.Edges = append(plan.Edges, storage.Edge{
				RunID: runID, Parent: plan.Nodes[index-1].ID, Child: nodeID, ParentOrder: 0,
			})
		}
	}

	plan.Run.TerminalNodeID = plan.Nodes[count-1].ID

	return plan
}
