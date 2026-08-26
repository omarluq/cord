package sqlite_test

import (
	"database/sql"
	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStore_InspectRunSnapshotAndCounts(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	now := time.Date(2025, time.January, 2, 3, 4, 5, 6000000, time.FixedZone("test", 2*60*60))
	plan := validPlan(now, "inspect-current")
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

func TestStore_InspectRunFailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*testing.T, *sql.DB, storage.RunID)
		name   string
	}{
		{
			name: "missing terminal reason",
			mutate: func(t *testing.T, database *sql.DB, runID storage.RunID) {
				t.Helper()
				_, err := database.ExecContext(t.Context(), `UPDATE cord_runs
					SET status = 'failed', completed_at = updated_at
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
