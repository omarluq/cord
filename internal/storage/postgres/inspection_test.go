package postgres_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	postgresstore "github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresInspectionAndNodePagination(t *testing.T) {
	t.Parallel()

	database := openInspectionPostgres(t)
	store, err := postgresstore.New(database)
	require.NoError(t, err)

	now := time.Date(2026, 8, 24, 1, 0, 0, 123456000, time.FixedZone("test", 2*60*60))
	insertInspectionRun(t, database, &inspectionRun{
		runID: "inspect", status: storage.RunRunning, reason: nil, now: now, finishedAt: nil,
	})

	for index, status := range []storage.NodeStatus{
		storage.NodeReady, storage.NodePending, storage.NodeReady,
	} {
		insertInspectionNode(t, database, "inspect", fmt.Sprintf("node-%d", index), status, now)
	}

	report, err := store.InspectRun(t.Context(), "inspect")
	require.NoError(t, err)
	assert.Equal(t, "UTC", report.SubmittedAt.Location().String())
	assert.Equal(t, storage.NodeStateCounts{
		Pending: 1, Ready: 2, Running: 0, RetryWait: 0, Completed: 0, Failed: 0, Canceled: 0,
	}, report.NodeCounts)

	page, err := store.ListRunNodes(t.Context(), "inspect", storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: "", PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 2)
	assert.Equal(t, storage.NodeID("node-0"), page.Nodes[0].NodeID)
	assert.Equal(t, "node-1", page.ContinuationToken)

	page, err = store.ListRunNodes(t.Context(), "inspect", storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: page.ContinuationToken, PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 1)
	assert.Equal(t, storage.NodeID("node-2"), page.Nodes[0].NodeID)
	assert.Empty(t, page.ContinuationToken)

	ready := storage.NodeReady
	page, err = store.ListRunNodes(t.Context(), "inspect", storage.NodeQuery{
		State: &ready, Reason: nil, ContinuationToken: "", PageSize: 0,
	})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 2)
	assert.Equal(t, storage.NodeID("node-0"), page.Nodes[0].NodeID)
	assert.Equal(t, storage.NodeID("node-2"), page.Nodes[1].NodeID)

	_, err = store.ListRunNodes(t.Context(), "missing", storage.NodeQuery{})
	require.ErrorIs(t, err, storage.ErrRunNotFound)

	completed := storage.NodeCompleted
	page, err = store.ListRunNodes(t.Context(), "inspect", storage.NodeQuery{
		State: &completed, Reason: nil, ContinuationToken: "", PageSize: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, page.Nodes)
}

func openInspectionPostgres(t *testing.T) *sql.DB {
	t.Helper()
	database := openPostgres(t, startPostgres(t))
	require.NoError(t, postgresstore.Migrate(t.Context(), database))

	return database
}

type inspectionRun struct {
	reason     *string
	finishedAt *time.Time
	now        time.Time
	runID      storage.RunID
	status     storage.RunStatus
}

func insertInspectionRun(t *testing.T, database *sql.DB, run *inspectionRun) {
	t.Helper()

	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, terminal_node_id,
		created_at, updated_at, completed_at, max_attempts, retry_base_delay_ns,
		retry_max_delay_ns, retry_policy_version, terminal_reason
	) VALUES ($1, 'inspection', 'hash', $2, ''::bytea, 'node', $3, $3, $4, 3, 1, 1, 1, $5)`,
		run.runID, run.status, run.now, run.finishedAt, run.reason)
	require.NoError(t, err)
}

func insertInspectionNode(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	nodeID string,
	status storage.NodeStatus,
	now time.Time,
) {
	t.Helper()

	stateChangedAt := now

	var finishedAt any
	if terminal, _ := status.Terminal(); terminal {
		finishedAt = now
	}

	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_generation, completed_at, state_changed_at, terminal_reason
	) VALUES ($1, $2, 'inspection.node', 'signature', $3, 0, 0, $4, 0, $5, $6, $7)`,
		runID, nodeID, status, now, finishedAt, stateChangedAt, nil)
	require.NoError(t, err)
}
