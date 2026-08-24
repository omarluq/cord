package postgres_test

import (
	"database/sql"
	"fmt"
	"strings"
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
		runID: "inspect", status: storage.RunRunning, reason: nil, now: now, finishedAt: nil, version: 1,
	})

	for index, status := range []storage.NodeStatus{
		storage.NodeReady, storage.NodePending, storage.NodeReady,
	} {
		insertInspectionNode(t, database, "inspect", fmt.Sprintf("node-%d", index), status, 1, now)
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

func TestPostgresInspectionLegacyAndFailClosed(t *testing.T) {
	t.Parallel()

	database := openInspectionPostgres(t)
	store, err := postgresstore.New(database)
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Microsecond)

	insertInspectionRun(t, database, &inspectionRun{
		runID: "legacy", status: storage.RunFailed, reason: nil, now: now, finishedAt: &now, version: 0,
	})
	insertInspectionNode(t, database, "legacy", "failed", storage.NodeFailed, 0, now)
	insertInspectionNode(t, database, "legacy", "sibling", storage.NodeCanceled, 0, now)

	report, err := store.InspectRun(t.Context(), "legacy")
	require.NoError(t, err)
	assert.Equal(t, storage.ReasonLegacyUnknown, report.Reason)

	page, err := store.ListRunNodes(t.Context(), "legacy", storage.NodeQuery{})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 2)
	assert.Equal(t, storage.ReasonLegacyUnknown, page.Nodes[0].Reason)
	assert.Equal(t, storage.ReasonCanceledByRunFailure, page.Nodes[1].Reason)

	insertInspectionRun(t, database, &inspectionRun{
		runID: "future", status: storage.RunRunning, reason: nil, now: now, finishedAt: nil, version: 2,
	})
	insertInspectionNode(t, database, "future", "node", storage.NodeReady, 2, now)
	_, err = store.InspectRun(t.Context(), "future")
	require.ErrorIs(t, err, storage.ErrRunIncompatible)
	_, err = store.ListRunNodes(t.Context(), "future", storage.NodeQuery{})
	require.ErrorIs(t, err, storage.ErrRunIncompatible)

	badReason := string(storage.ReasonFailureLeaseExpired)
	insertInspectionRun(t, database, &inspectionRun{
		runID: "malformed", status: storage.RunRunning, reason: &badReason, now: now,
		finishedAt: nil, version: 1,
	})
	insertInspectionNode(t, database, "malformed", "node", storage.NodeReady, 1, now)
	_, err = store.InspectRun(t.Context(), "malformed")
	require.ErrorIs(t, err, storage.ErrRunIncompatible)
}

// TestPostgresInspectionLegacyRunningNode verifies legacy running-node runner metadata.
func TestPostgresInspectionLegacyRunningNode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lastRunner any
		name       string
		wantError  bool
	}{
		{name: "lease owner supplies report runner", lastRunner: nil, wantError: false},
		{name: "persisted last runner is incompatible", lastRunner: "runner", wantError: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database := openInspectionPostgres(t)
			store, err := postgresstore.New(database)
			require.NoError(t, err)

			now := time.Now().UTC().Truncate(time.Microsecond)
			insertInspectionRun(t, database, &inspectionRun{
				runID: "legacy-running", status: storage.RunRunning, reason: nil,
				finishedAt: nil, now: now, version: 0,
			})

			_, err = database.ExecContext(t.Context(), `INSERT INTO cord_nodes (
				run_id, node_id, function_key, signature_hash, status, remaining_deps,
				attempt, available_at, lease_owner, lease_generation, lease_expires_at,
				started_at, last_runner_id
			) VALUES (
				'legacy-running', 'node', 'inspection.node', 'signature', 'running', 0,
				1, $1, 'runner', 1, $2, $1, $3
			)`, now, now.Add(time.Minute), testCase.lastRunner)
			require.NoError(t, err)

			page, err := store.ListRunNodes(t.Context(), "legacy-running", storage.NodeQuery{})
			if testCase.wantError {
				require.ErrorIs(t, err, storage.ErrRunIncompatible)

				return
			}

			require.NoError(t, err)
			require.Len(t, page.Nodes, 1)
			require.NotNil(t, page.Nodes[0].RunnerID)
			assert.Equal(t, storage.RunnerID("runner"), *page.Nodes[0].RunnerID)
			require.NotNil(t, page.Nodes[0].CurrentLease)
			assert.Equal(t, storage.RunnerID("runner"), page.Nodes[0].CurrentLease.RunnerID)
		})
	}
}

func TestPostgresInspectionUsesRunScopedCountPlan(t *testing.T) {
	t.Parallel()

	database := prepareInspectionPlanDatabase(t)

	const explain = `EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT
		r.id, r.workflow_name, r.status, r.lifecycle_version, r.terminal_reason,
		r.created_at, r.started_at, r.updated_at, r.completed_at, r.terminal_runner_id,
		counts.pending, counts.ready, counts.running, counts.retry_wait,
		counts.completed, counts.failed, counts.canceled, counts.total
	FROM cord_runs r
	CROSS JOIN LATERAL (
		SELECT
			COUNT(*) FILTER (WHERE n.status = 'pending') AS pending,
			COUNT(*) FILTER (WHERE n.status = 'ready') AS ready,
			COUNT(*) FILTER (WHERE n.status = 'running') AS running,
			COUNT(*) FILTER (WHERE n.status = 'retry_wait') AS retry_wait,
			COUNT(*) FILTER (WHERE n.status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE n.status = 'failed') AS failed,
			COUNT(*) FILTER (WHERE n.status = 'canceled') AS canceled,
			COUNT(*) AS total
		FROM cord_nodes n
		WHERE n.run_id = r.id
	) counts
	WHERE r.id = $1`

	plan := explainPostgres(t, database, explain, "plan")
	assert.Contains(t, plan, "cord_nodes_run_status_idx")
	assert.Contains(t, plan, "Index Cond: (run_id = r.id)")
	assert.NotContains(t, plan, "Seq Scan on cord_nodes")
	assert.NotContains(t, plan, "Rows Removed by Filter")
}

func TestPostgresNodePageUsesPrimaryKeyKeysetPlan(t *testing.T) {
	t.Parallel()

	database := prepareInspectionPlanDatabase(t)

	const explain = `EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT
		n.run_id, n.node_id, n.function_key, n.status, n.lifecycle_version, n.terminal_reason,
		n.attempt, r.max_attempts, n.available_at, n.started_at, n.last_started_at,
		n.state_changed_at, n.completed_at, n.last_runner_id,
		n.lease_owner, n.lease_generation, n.lease_expires_at
	FROM cord_nodes n
	JOIN cord_runs r ON r.id = n.run_id
	WHERE n.run_id = $1
		AND n.node_id > $2
		AND ($3::text IS NULL OR n.status = $3)
		AND ($4::text IS NULL OR CASE
			WHEN n.lifecycle_version IS NOT NULL THEN n.terminal_reason
			WHEN n.status = 'completed' THEN 'succeeded'
			WHEN n.status = 'failed' THEN 'legacy_unknown'
			WHEN n.status = 'canceled' AND r.status = 'failed' THEN 'canceled_by_run_failure'
			WHEN n.status = 'canceled' THEN 'legacy_unknown'
			ELSE NULL
		END = $4)
	ORDER BY n.node_id
	LIMIT $5`

	tests := []struct {
		state  any
		reason any
		name   string
	}{
		{name: "unfiltered", state: nil, reason: nil},
		{name: "state", state: storage.NodeReady, reason: nil},
		{name: "reason", state: nil, reason: storage.ReasonSucceeded},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			plan := explainPostgres(t, database, explain,
				"plan", "node-005", testCase.state, testCase.reason, 6)
			assert.Contains(t, plan, "cord_nodes_pkey")
			assert.Contains(t, plan, "node_id > 'node-005'::text")
			assert.NotContains(t, strings.ToLower(plan), "sort")
		})
	}
}

func explainPostgres(t *testing.T, database *sql.DB, statement string, arguments ...any) string {
	t.Helper()

	transaction, err := database.BeginTx(t.Context(), nil)

	require.NoError(t, err)
	defer func() { require.NoError(t, transaction.Rollback()) }()

	_, err = transaction.ExecContext(t.Context(), "SET LOCAL enable_seqscan = off")
	require.NoError(t, err)

	rows, err := transaction.QueryContext(t.Context(), statement, arguments...)

	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var plan strings.Builder

	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}

	require.NoError(t, rows.Err())

	return plan.String()
}

func prepareInspectionPlanDatabase(t *testing.T) *sql.DB {
	t.Helper()

	database := openInspectionPostgres(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	insertInspectionRun(t, database, &inspectionRun{
		runID: "plan", status: storage.RunRunning, reason: nil, now: now, finishedAt: nil, version: 1,
	})

	for index := range 20 {
		insertInspectionNode(t, database, "plan", fmt.Sprintf("node-%03d", index), storage.NodeReady, 1, now)
	}

	analyzeInspectionTables(t, database)

	return database
}

func analyzeInspectionTables(t *testing.T, database *sql.DB) {
	t.Helper()

	_, err := database.ExecContext(t.Context(), "ANALYZE cord_runs, cord_nodes")
	require.NoError(t, err)
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
	version    int
}

func insertInspectionRun(t *testing.T, database *sql.DB, run *inspectionRun) {
	t.Helper()

	var lifecycleVersion any
	if run.version != 0 {
		lifecycleVersion = run.version
	}

	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, terminal_node_id,
		created_at, updated_at, completed_at, max_attempts, retry_base_delay_ns,
		retry_max_delay_ns, retry_policy_version, lifecycle_version, terminal_reason
	) VALUES ($1, 'inspection', 'hash', $2, ''::bytea, 'node', $3, $3, $4, 3, 1, 1, 1, $5, $6)`,
		run.runID, run.status, run.now, run.finishedAt, lifecycleVersion, run.reason)
	require.NoError(t, err)
}

func insertInspectionNode(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	nodeID string,
	status storage.NodeStatus,
	version int,
	now time.Time,
) {
	t.Helper()

	var lifecycleVersion, stateChangedAt any
	if version != 0 {
		lifecycleVersion = version
		stateChangedAt = now
	}

	var finishedAt any
	if terminal, _ := status.Terminal(); terminal {
		finishedAt = now
	}

	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_generation, completed_at, lifecycle_version,
		state_changed_at, terminal_reason
	) VALUES ($1, $2, 'inspection.node', 'signature', $3, 0, 0, $4, 0, $5, $6, $7, $8)`,
		runID, nodeID, status, now, finishedAt, lifecycleVersion, stateChangedAt, nil)
	require.NoError(t, err)
}
