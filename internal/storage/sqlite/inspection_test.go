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

func TestStore_ListRunNodesLegacyRunningNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lastRunner any
		name       string
		wantError  bool
	}{
		{name: "lease owner supplies report runner", lastRunner: nil, wantError: false},
		{name: "persisted last runner is incompatible", lastRunner: "runner", wantError: true},
		{name: "persisted empty last runner is incompatible", lastRunner: "", wantError: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			now := time.Now().UTC()
			plan := validPlan(now, "legacy-running")
			requireCreateRun(t.Context(), t, store, &plan)

			_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
				SET status = ?, attempt = 1, lease_owner = 'runner', lease_generation = 1,
					lease_expires_at = ?, started_at = available_at, lifecycle_version = NULL,
					state_changed_at = NULL, last_started_at = NULL, last_runner_id = ?
				WHERE run_id = ? AND node_id = ?`,
				storage.NodeRunning, now.Add(time.Minute).Format(time.RFC3339Nano), testCase.lastRunner,
				plan.Run.ID, compileNode)
			require.NoError(t, err)

			page, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
			if testCase.wantError {
				require.ErrorIs(t, err, storage.ErrRunIncompatible)

				return
			}

			require.NoError(t, err)
			require.Len(t, page.Nodes, 2)
			require.NotNil(t, page.Nodes[0].RunnerID)
			assert.Equal(t, storage.RunnerID("runner"), *page.Nodes[0].RunnerID)
			require.NotNil(t, page.Nodes[0].CurrentLease)
			assert.Equal(t, storage.RunnerID("runner"), page.Nodes[0].CurrentLease.RunnerID)
		})
	}
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

func TestStore_ListRunNodesValidatesCurrentNodeStartMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                               string
		clearFirst, clearLast, clearRunner bool
	}{
		{name: "missing first start", clearFirst: true, clearLast: false, clearRunner: false},
		{name: "missing last start", clearFirst: false, clearLast: true, clearRunner: false},
		{name: "missing runner", clearFirst: false, clearLast: false, clearRunner: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("start-metadata-"+testCase.name))
			requireCreateRun(t.Context(), t, store, &plan)

			_, claimed, err := store.ClaimReadyNode(t.Context(), "worker", time.Minute)
			require.NoError(t, err)
			require.True(t, claimed)

			_, err = database.ExecContext(t.Context(), `UPDATE cord_nodes
				SET status = 'retry_wait', lease_owner = NULL, lease_expires_at = NULL,
					started_at = CASE WHEN ? THEN NULL ELSE started_at END,
					last_started_at = CASE WHEN ? THEN NULL ELSE last_started_at END,
					last_runner_id = CASE WHEN ? THEN NULL ELSE last_runner_id END
				WHERE run_id = ? AND node_id = ?`,
				testCase.clearFirst, testCase.clearLast, testCase.clearRunner,
				plan.Run.ID, plan.Nodes[0].ID)
			require.NoError(t, err)

			_, err = store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
			assert.ErrorIs(t, err, storage.ErrRunIncompatible)
		})
	}
}

func TestStore_ListRunNodesAllowsUnclaimedCurrentNode(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "unclaimed-current-node")
	requireCreateRun(t.Context(), t, store, &plan)
	_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET lifecycle_version = 1, state_changed_at = available_at
		WHERE run_id = ?`, plan.Run.ID)
	require.NoError(t, err)

	page, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
	require.NoError(t, err)
	require.Len(t, page.Nodes, len(plan.Nodes))

	for _, node := range page.Nodes {
		assert.Zero(t, node.Attempt)
		assert.Nil(t, node.FirstStartedAt)
		assert.Nil(t, node.LastStartedAt)
		assert.Nil(t, node.RunnerID)
	}
}

func TestStore_ListRunNodesRejectsUnclaimedCurrentNodeStartMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                          string
		firstStart, lastStart, runner bool
	}{
		{name: "first start", firstStart: true, lastStart: false, runner: false},
		{name: "last start", firstStart: false, lastStart: true, runner: false},
		{name: "runner", firstStart: false, lastStart: false, runner: true},
		{name: "complete start metadata", firstStart: true, lastStart: true, runner: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("unclaimed-metadata-"+testCase.name))
			requireCreateRun(t.Context(), t, store, &plan)

			_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
				SET lifecycle_version = 1, state_changed_at = available_at,
					started_at = CASE WHEN ? THEN available_at ELSE NULL END,
					last_started_at = CASE WHEN ? THEN available_at ELSE NULL END,
					last_runner_id = CASE WHEN ? THEN 'worker' ELSE NULL END
				WHERE run_id = ? AND node_id = ?`,
				testCase.firstStart, testCase.lastStart, testCase.runner,
				plan.Run.ID, compileNode)
			require.NoError(t, err)

			_, err = store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
			assert.ErrorIs(t, err, storage.ErrRunIncompatible)
		})
	}
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
	plan := validPlan(time.Now().UTC(), "invalid-filters")
	requireCreateRun(t.Context(), t, store, &plan)

	unknown := storage.NodeStatus("unknown")
	state := storage.NodeReady
	reason := storage.ReasonSucceeded
	tests := []struct {
		name      string
		wantError string
		query     storage.NodeQuery
	}{
		{
			name: "unknown state",
			query: storage.NodeQuery{
				State: &unknown, Reason: nil, ContinuationToken: "", PageSize: 0,
			},
			wantError: `list run nodes: unknown state "unknown"`,
		},
		{
			name: "state reason mismatch",
			query: storage.NodeQuery{
				State: &state, Reason: &reason, ContinuationToken: "", PageSize: 0,
			},
			wantError: `list run nodes: reason "succeeded" is invalid for state "ready"`,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := store.ListRunNodes(t.Context(), plan.Run.ID, testCase.query)
			require.EqualError(t, err, testCase.wantError)
		})
	}
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
