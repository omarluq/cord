package cord_test

import (
	"database/sql"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowGetReadsLegacyTerminalRowsAfterMigration(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	removeLifecycleSchema(t, database)

	fixture, err := os.ReadFile("testdata/legacy_terminal_runs.sql")
	require.NoError(t, err)

	fixtureSQL := string(fixture)
	fixtureSQL = regexp.MustCompile(`decode\('([0-9A-F]+)', 'hex'\)`).ReplaceAllString(fixtureSQL, "X'$1'")
	fixtureSQL = strings.ReplaceAll(fixtureSQL, " AS TIMESTAMP", " AS TEXT")
	_, err = database.ExecContext(t.Context(), fixtureSQL)
	require.NoError(t, err)

	before := readLegacyTerminalRows(t, database)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	assert.Equal(t, before, readLegacyTerminalRows(t, database),
		"migration must not rewrite historical terminal data")
	assertLegacyLifecycleNull(t, database)

	runtime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	flow := assertLegacyActiveSnapshot(t, runtime)
	assertLegacyTransitionUsesVersionOne(t, database)
	assertLegacyTerminalSnapshots(t, runtime)
	assertLegacyGetResults(t, flow)
}

func assertLegacyActiveSnapshot(t *testing.T, runtime *cord.Cord) cord.Workflow[int, int] {
	t.Helper()

	active, err := runtime.InspectRun(t.Context(), "legacy-active")
	require.NoError(t, err)
	assert.Equal(t, cord.RunStateRunning, active.State)
	assert.Empty(t, active.Reason)
	assert.Nil(t, active.FirstStartedAt)
	assert.Nil(t, active.FinishedAt)
	assert.Equal(t, cord.NodeStateCounts{Ready: 1}, active.NodeCounts)

	activeNodes, err := runtime.ListRunNodes(t.Context(), "legacy-active", cord.NodeQuery{})
	require.NoError(t, err)
	require.Len(t, activeNodes.Nodes, 1)
	assert.Equal(t, cord.NodeStateReady, activeNodes.Nodes[0].State)
	assert.Empty(t, activeNodes.Nodes[0].Reason)
	assert.Zero(t, activeNodes.Nodes[0].Attempt)
	assert.Nil(t, activeNodes.Nodes[0].StateChangedAt)
	assert.Nil(t, activeNodes.Nodes[0].LastStartedAt)
	assert.Nil(t, activeNodes.Nodes[0].RunnerID)

	flow := runtime.From("legacy-terminal-public-get", addOne)
	activeResult, err := flow.Get(t.Context(), "legacy-active")
	require.NoError(t, err)
	assert.Equal(t, 42, activeResult)

	return flow
}

func assertLegacyTerminalSnapshots(t *testing.T, runtime *cord.Cord) {
	t.Helper()

	tests := []struct {
		runID     cord.RunID
		state     cord.RunState
		reason    cord.TerminalReason
		nodeState cord.NodeState
	}{
		{runID: "legacy-completed", state: cord.RunStateCompleted, reason: cord.ReasonSucceeded},
		{
			runID:     "legacy-failed",
			state:     cord.RunStateFailed,
			reason:    cord.ReasonLegacyUnknown,
			nodeState: cord.NodeStateFailed,
		},
		{
			runID:     "legacy-canceled",
			state:     cord.RunStateCanceled,
			reason:    cord.ReasonCanceledByRequest,
			nodeState: cord.NodeStateCanceled,
		},
	}

	for _, test := range tests {
		snapshot, err := runtime.InspectRun(t.Context(), test.runID)
		require.NoError(t, err)
		assert.Equal(t, test.state, snapshot.State)

		if test.reason == cord.ReasonLegacyUnknown {
			assert.Equal(t, test.reason, snapshot.Reason,
				"legacy failures must not be classified by parsing their message")
		} else {
			assert.Equal(t, test.reason, snapshot.Reason)
		}

		if test.nodeState != "" {
			assertLegacyTerminalNode(t, runtime, test.runID, test.nodeState)
		}
	}
}

func assertLegacyTerminalNode(t *testing.T, runtime *cord.Cord, runID cord.RunID, state cord.NodeState) {
	t.Helper()

	page, err := runtime.ListRunNodes(t.Context(), runID, cord.NodeQuery{})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 1)
	assert.Equal(t, state, page.Nodes[0].State)
	assert.Equal(t, cord.ReasonLegacyUnknown, page.Nodes[0].Reason)
}

func assertLegacyGetResults(t *testing.T, flow cord.Workflow[int, int]) {
	t.Helper()

	result, err := flow.Get(t.Context(), "legacy-completed")
	require.NoError(t, err)
	assert.Equal(t, 42, result)

	_, err = flow.Get(t.Context(), "legacy-failed")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "legacy boom")

	_, err = flow.Get(t.Context(), "legacy-canceled")
	require.ErrorIs(t, err, cord.ErrRunCanceled)
}

func removeLifecycleSchema(t *testing.T, database *sql.DB) {
	t.Helper()

	for _, statement := range []string{
		`ALTER TABLE cord_runs DROP COLUMN lifecycle_version`,
		`ALTER TABLE cord_runs DROP COLUMN started_at`,
		`ALTER TABLE cord_runs DROP COLUMN terminal_reason`,
		`ALTER TABLE cord_runs DROP COLUMN terminal_runner_id`,
		`ALTER TABLE cord_nodes DROP COLUMN lifecycle_version`,
		`ALTER TABLE cord_nodes DROP COLUMN state_changed_at`,
		`ALTER TABLE cord_nodes DROP COLUMN last_started_at`,
		`ALTER TABLE cord_nodes DROP COLUMN last_runner_id`,
		`ALTER TABLE cord_nodes DROP COLUMN terminal_reason`,
		`DELETE FROM cord_schema_migrations WHERE version_id = 5`,
	} {
		_, err := database.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}
}

func assertLegacyTransitionUsesVersionOne(t *testing.T, database *sql.DB) {
	t.Helper()

	var runVersion, nodeVersion sql.NullInt64

	err := database.QueryRowContext(t.Context(), `SELECT r.lifecycle_version, n.lifecycle_version
		FROM cord_runs r JOIN cord_nodes n ON n.run_id = r.id
		WHERE r.id = 'legacy-active'`).Scan(&runVersion, &nodeVersion)
	require.NoError(t, err)
	require.True(t, runVersion.Valid)
	require.True(t, nodeVersion.Valid)
	assert.EqualValues(t, 1, runVersion.Int64)
	assert.EqualValues(t, 1, nodeVersion.Int64)
}

func assertLegacyLifecycleNull(t *testing.T, database *sql.DB) {
	t.Helper()

	var nonNullValues int

	err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM cord_runs r
		JOIN cord_nodes n ON n.run_id = r.id
		WHERE r.lifecycle_version IS NOT NULL OR r.started_at IS NOT NULL
		OR r.terminal_reason IS NOT NULL OR r.terminal_runner_id IS NOT NULL
		OR n.lifecycle_version IS NOT NULL OR n.state_changed_at IS NOT NULL
		OR n.last_started_at IS NOT NULL OR n.last_runner_id IS NOT NULL
		OR n.terminal_reason IS NOT NULL`).Scan(&nonNullValues)
	require.NoError(t, err)
	assert.Zero(t, nonNullValues, "migration must not invent lifecycle history")
}

type legacyTerminalRow struct {
	StartedAt             string
	RunFailure            string
	DefinitionHash        string
	RunStatus             string
	Input                 string
	Output                string
	NodeCompletedAt       string
	TerminalNodeID        string
	CreatedAt             string
	UpdatedAt             string
	WorkflowName          string
	CompletedAt           string
	SubmissionFingerprint string
	NodeFailure           string
	RunID                 string
	IdempotencyKey        string
	LeaseOwner            string
	NodeID                string
	FunctionKey           string
	SignatureHash         string
	NodeStatus            string
	NodeOutput            string
	LeaseExpiresAt        string
	AvailableAt           string
	RetryPolicyVersion    int
	LeaseGeneration       int64
	Attempt               int
	RemainingDeps         int
	RetryMaxDelayNS       int64
	RetryBaseDelayNS      int64
	MaxAttempts           int
}

func readLegacyTerminalRows(t *testing.T, database *sql.DB) []legacyTerminalRow {
	t.Helper()

	rows, err := database.QueryContext(t.Context(), `SELECT
		r.id, r.workflow_name, r.definition_hash, r.status,
		quote(r.input_payload), quote(r.output_payload), quote(r.error_payload),
		r.terminal_node_id, quote(r.created_at), quote(r.updated_at), quote(r.completed_at),
		r.max_attempts, r.retry_base_delay_ns, r.retry_max_delay_ns, r.retry_policy_version,
		quote(r.idempotency_key), quote(r.submission_fingerprint),
		n.node_id, n.function_key, n.signature_hash, n.status, n.remaining_deps, n.attempt,
		quote(n.available_at), quote(n.lease_owner), n.lease_generation,
		quote(n.lease_expires_at), quote(n.output_payload), quote(n.error_payload),
		quote(n.started_at), quote(n.completed_at)
		FROM cord_runs r JOIN cord_nodes n ON n.run_id = r.id
		ORDER BY r.id, n.node_id`)
	require.NoError(t, err)

	defer func() { require.NoError(t, rows.Close()) }()

	var result []legacyTerminalRow

	for rows.Next() {
		var row legacyTerminalRow
		require.NoError(t, rows.Scan(
			&row.RunID, &row.WorkflowName, &row.DefinitionHash, &row.RunStatus,
			&row.Input, &row.Output, &row.RunFailure, &row.TerminalNodeID,
			&row.CreatedAt, &row.UpdatedAt, &row.CompletedAt, &row.MaxAttempts,
			&row.RetryBaseDelayNS, &row.RetryMaxDelayNS, &row.RetryPolicyVersion,
			&row.IdempotencyKey, &row.SubmissionFingerprint,
			&row.NodeID, &row.FunctionKey, &row.SignatureHash, &row.NodeStatus,
			&row.RemainingDeps, &row.Attempt, &row.AvailableAt, &row.LeaseOwner,
			&row.LeaseGeneration, &row.LeaseExpiresAt, &row.NodeOutput,
			&row.NodeFailure, &row.StartedAt, &row.NodeCompletedAt,
		))
		result = append(result, row)
	}

	require.NoError(t, rows.Err())

	return result
}
