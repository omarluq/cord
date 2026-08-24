package sqlite_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateConcurrentConnections(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "concurrent.db")
	first := openZeroTimeoutDatabase(t, path)
	second := openZeroTimeoutDatabase(t, path)

	start := make(chan struct{})
	results := make(chan error, 2)

	var waitGroup sync.WaitGroup
	for _, database := range []*sql.DB{first, second} {
		waitGroup.Go(func() {
			<-start

			results <- sqlite.Migrate(t.Context(), database)
		})
	}

	close(start)
	waitGroup.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}

	require.NoError(t, sqlite.Verify(t.Context(), first))
	require.NoError(t, sqlite.Verify(t.Context(), second))

	rows, err := first.QueryContext(t.Context(), `SELECT version_id, COUNT(*)
		FROM cord_schema_migrations WHERE is_applied = 1 AND version_id > 0
		GROUP BY version_id ORDER BY version_id`)

	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var versions []int64

	for rows.Next() {
		var (
			version int64
			applied int
		)

		require.NoError(t, rows.Scan(&version, &applied))
		assert.Equal(t, 1, applied)

		versions = append(versions, version)
	}

	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, versions)
}

func TestVerifyReportsSchemaCompatibility(t *testing.T) {
	t.Parallel()

	t.Run("absent", func(t *testing.T) {
		t.Parallel()

		database := openDatabase(t, true)
		err := sqlite.Verify(t.Context(), database)
		require.ErrorIs(t, err, storage.ErrSchemaOutdated)
	})

	t.Run("current", func(t *testing.T) {
		t.Parallel()

		database := openDatabase(t, true)
		require.NoError(t, sqlite.Migrate(t.Context(), database))
		require.NoError(t, sqlite.Verify(t.Context(), database))
	})

	t.Run("newer", func(t *testing.T) {
		t.Parallel()

		database := openDatabase(t, true)
		require.NoError(t, sqlite.Migrate(t.Context(), database))
		_, err := database.ExecContext(t.Context(), `INSERT INTO cord_schema_migrations
			(version_id, is_applied, tstamp) VALUES (6, 1, datetime('now'))`)
		require.NoError(t, err)
		err = sqlite.Verify(t.Context(), database)
		require.ErrorIs(t, err, storage.ErrSchemaNewer)
		assert.Contains(t, err.Error(), "current=6 required=5")
	})
}

const dropEdges = `DROP TABLE cord_edges`

type incompatibleSchemaCase struct {
	name       string
	wantError  string
	statements []string
}

func TestVerifyRejectsStructurallyIncompatibleCurrentSchema(t *testing.T) {
	t.Parallel()

	for _, testCase := range incompatibleSchemaCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			runIncompatibleSchemaCase(t, testCase)
		})
	}
}

func incompatibleSchemaCases() []incompatibleSchemaCase {
	cases := baseIncompatibleSchemaCases()
	cases = append(cases, identityIncompatibleSchemaCases()...)

	return append(cases, lifecycleIncompatibleSchemaCases()...)
}

func baseIncompatibleSchemaCases() []incompatibleSchemaCase {
	return []incompatibleSchemaCase{
		{
			name:       "missing column",
			statements: []string{`ALTER TABLE cord_runs RENAME COLUMN max_attempts TO missing_max_attempts`},
			wantError:  `column "cord_runs"."max_attempts"`,
		},
		{
			name: "wrong column affinity",
			statements: []string{
				dropEdges,
				`CREATE TABLE cord_edges (
					run_id TEXT NOT NULL,
					parent_node_id TEXT NOT NULL,
					child_node_id TEXT NOT NULL,
					parent_order TEXT NOT NULL DEFAULT 0,
					PRIMARY KEY (run_id, parent_node_id, child_node_id),
					FOREIGN KEY (run_id, parent_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE,
					FOREIGN KEY (run_id, child_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE
				)`,
			},
			wantError: `column "cord_edges"."parent_order"`,
		},
		{
			name: "missing primary key",
			statements: []string{
				dropEdges,
				`CREATE TABLE cord_edges (
					run_id TEXT NOT NULL,
					parent_node_id TEXT NOT NULL,
					child_node_id TEXT NOT NULL,
					parent_order INTEGER NOT NULL DEFAULT 0,
					FOREIGN KEY (run_id, parent_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE,
					FOREIGN KEY (run_id, child_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE
				)`,
			},
			wantError: `column "cord_edges"."run_id"`,
		},
		{
			name:       "missing node index",
			statements: []string{`DROP INDEX cord_nodes_lease_expires_at_idx`},
			wantError:  `index "cord_nodes_lease_expires_at_idx" is missing`,
		},
		{
			name:       "missing ordered-child edge index",
			statements: []string{`DROP INDEX cord_edges_run_child_parent_order_idx`},
			wantError:  `index "cord_edges_run_child_parent_order_idx" is missing`,
		},
		{
			name: "wrong index columns",
			statements: []string{
				`DROP INDEX cord_nodes_run_status_idx`,
				`CREATE INDEX cord_nodes_run_status_idx ON cord_nodes(status, run_id)`,
			},
			wantError: `index "cord_nodes_run_status_idx" has columns [status run_id]`,
		},
		{
			name: "partial required index",
			statements: []string{
				`DROP INDEX cord_nodes_run_status_idx`,
				`CREATE INDEX cord_nodes_run_status_idx ON cord_nodes(run_id, status) WHERE status = 'running'`,
			},
			wantError: `index "cord_nodes_run_status_idx" has columns [run_id status], unique=false, and partial=true`,
		},
		{
			name: "missing foreign keys",
			statements: []string{
				dropEdges,
				`CREATE TABLE cord_edges (
					run_id TEXT NOT NULL,
					parent_node_id TEXT NOT NULL,
					child_node_id TEXT NOT NULL,
					parent_order INTEGER NOT NULL DEFAULT 0,
					PRIMARY KEY (run_id, parent_node_id, child_node_id)
				)`,
				`CREATE INDEX cord_edges_run_child_parent_order_idx
					ON cord_edges(run_id, child_node_id, parent_order)`,
			},
			wantError: `table "cord_edges" is missing foreign key`,
		},
	}
}

func identityIncompatibleSchemaCases() []incompatibleSchemaCase {
	return []incompatibleSchemaCase{
		{
			name:       "missing submission identity column",
			statements: []string{`ALTER TABLE cord_runs RENAME COLUMN submission_fingerprint TO missing_fingerprint`},
			wantError:  `column "cord_runs"."submission_fingerprint"`,
		},
		{
			name:       "missing idempotency index",
			statements: []string{`DROP INDEX cord_runs_workflow_name_idempotency_key_idx`},
			wantError:  `index "cord_runs_workflow_name_idempotency_key_idx" is missing`,
		},
		{
			name: "non-unique idempotency index",
			statements: []string{
				`DROP INDEX cord_runs_workflow_name_idempotency_key_idx`,
				`CREATE INDEX cord_runs_workflow_name_idempotency_key_idx
					ON cord_runs(workflow_name, idempotency_key)`,
			},
			wantError: `index "cord_runs_workflow_name_idempotency_key_idx" has columns ` +
				`[workflow_name idempotency_key], unique=false`,
		},
	}
}

func lifecycleIncompatibleSchemaCases() []incompatibleSchemaCase {
	return []incompatibleSchemaCase{
		{
			name:       "missing run lifecycle column",
			statements: []string{`ALTER TABLE cord_runs RENAME COLUMN terminal_reason TO missing_terminal_reason`},
			wantError:  `column "cord_runs"."terminal_reason"`,
		},
		{
			name: "wrong lifecycle column affinity",
			statements: []string{
				`ALTER TABLE cord_nodes RENAME COLUMN lifecycle_version TO old_lifecycle_version`,
				`ALTER TABLE cord_nodes ADD COLUMN lifecycle_version TEXT`,
			},
			wantError: `column "cord_nodes"."lifecycle_version"`,
		},
		{
			name: "non-null lifecycle column with default",
			statements: []string{
				`ALTER TABLE cord_runs RENAME COLUMN lifecycle_version TO old_lifecycle_version`,
				`ALTER TABLE cord_runs ADD COLUMN lifecycle_version INTEGER NOT NULL DEFAULT 1`,
			},
			wantError: `column "cord_runs"."lifecycle_version"`,
		},
	}
}

func runIncompatibleSchemaCase(t *testing.T, testCase incompatibleSchemaCase) {
	t.Helper()
	database := openDatabase(t, false)
	require.NoError(t, sqlite.Migrate(t.Context(), database))

	for _, statement := range testCase.statements {
		_, err := database.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}

	err := sqlite.Verify(t.Context(), database)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema is incompatible")
	assert.Contains(t, err.Error(), testCase.wantError)
	require.NotErrorIs(t, err, storage.ErrSchemaOutdated)
	require.NotErrorIs(t, err, storage.ErrSchemaNewer)

	err = sqlite.Migrate(t.Context(), database)
	require.Error(t, err)
	assert.Contains(t, err.Error(), testCase.wantError)
}

func TestMigrateV5PreservesRowsFromEveryPriorSchema(t *testing.T) {
	t.Parallel()

	for version := int64(1); version < 5; version++ {
		for _, state := range priorSchemaLifecycleStates() {
			t.Run(fmt.Sprintf("v%d/%s-%s", version, state.runStatus, state.nodeStatus), func(t *testing.T) {
				t.Parallel()

				testMigrateV5PreservesPriorRow(t, version, state)
			})
		}
	}
}

type priorSchemaLifecycleState struct {
	runStatus  storage.RunStatus
	runReason  storage.TerminalReason
	nodeStatus storage.NodeStatus
	nodeReason storage.TerminalReason
}

func priorSchemaLifecycleStates() []priorSchemaLifecycleState {
	reasons := struct {
		succeeded, canceledByRequest, canceledByRunFailure, legacyUnknown storage.TerminalReason
	}{
		succeeded:            "succeeded",
		canceledByRequest:    "canceled_by_request",
		canceledByRunFailure: "canceled_by_run_failure",
		legacyUnknown:        "legacy_unknown",
	}

	return []priorSchemaLifecycleState{
		{runStatus: storage.RunRunning, runReason: "", nodeStatus: storage.NodePending, nodeReason: ""},
		{runStatus: storage.RunRunning, runReason: "", nodeStatus: storage.NodeReady, nodeReason: ""},
		{runStatus: storage.RunRunning, runReason: "", nodeStatus: storage.NodeRunning, nodeReason: ""},
		{runStatus: storage.RunRunning, runReason: "", nodeStatus: storage.NodeRetryWait, nodeReason: ""},
		{runStatus: storage.RunCanceling, runReason: "", nodeStatus: storage.NodeRunning, nodeReason: ""},
		{
			runStatus: storage.RunCompleted, runReason: reasons.succeeded,
			nodeStatus: storage.NodeCompleted, nodeReason: reasons.succeeded,
		},
		{
			runStatus: storage.RunFailed, runReason: reasons.legacyUnknown,
			nodeStatus: storage.NodeFailed, nodeReason: reasons.legacyUnknown,
		},
		{
			runStatus: storage.RunFailed, runReason: reasons.legacyUnknown,
			nodeStatus: storage.NodeCanceled, nodeReason: reasons.canceledByRunFailure,
		},
		{
			runStatus: storage.RunCanceled, runReason: reasons.canceledByRequest,
			nodeStatus: storage.NodeCanceled, nodeReason: reasons.legacyUnknown,
		},
	}
}

func testMigrateV5PreservesPriorRow(t *testing.T, version int64, state priorSchemaLifecycleState) {
	t.Helper()

	database := openDatabase(t, true)
	require.NoError(t, sqlite.MigrateToVersionForTest(t.Context(), database, version))

	insertPriorSchemaRow(t, database, state)
	beforeRun := readPriorRun(t, database, version)
	beforeNode := readPriorNode(t, database)

	err := sqlite.Verify(t.Context(), database)
	require.ErrorIs(t, err, storage.ErrSchemaOutdated)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	require.NoError(t, sqlite.Verify(t.Context(), database))

	assert.Equal(t, beforeRun, readPriorRun(t, database, 5))
	assert.Equal(t, beforeNode, readPriorNode(t, database))
	assertPriorLifecycleNull(t, database)

	store, err := sqlite.New(database)
	require.NoError(t, err)

	page, err := store.ListRunNodes(t.Context(), "legacy-run", storage.NodeQuery{})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 1)
	assert.Equal(t, state.nodeStatus, page.Nodes[0].State)
	assert.Equal(t, state.nodeReason, page.Nodes[0].Reason)

	report, err := store.InspectRun(t.Context(), "legacy-run")
	require.NoError(t, err)
	assert.Equal(t, state.runStatus, report.State)
	assert.Equal(t, state.runReason, report.Reason)
}

func insertPriorSchemaRow(t *testing.T, database *sql.DB, state priorSchemaLifecycleState) {
	t.Helper()

	terminal, _ := state.runStatus.Terminal()
	nodeTerminal, _ := state.nodeStatus.Terminal()
	attempt := 0

	var (
		runOutput, runError, runCompleted, nodeOutput, nodeError, nodeStarted, nodeCompleted any
		leaseOwner, leaseExpires                                                             any
	)

	leaseGeneration := 0

	if state.nodeStatus == storage.NodeRunning {
		attempt = 2
		leaseOwner = "legacy-owner"
		leaseGeneration = 7
		leaseExpires = "2099-01-02T03:06:05Z"
		nodeStarted = "2024-01-02T03:04:35Z"
	}

	if nodeTerminal {
		attempt = 2
		nodeStarted = "2024-01-02T03:04:35Z"
		nodeCompleted = "2024-01-02T05:06:07Z"

		if state.nodeStatus == storage.NodeCompleted {
			nodeOutput = []byte("node-output")
		}

		if state.nodeStatus == storage.NodeFailed {
			nodeError = []byte("node-error")
		}
	}

	if terminal {
		runCompleted = "2024-01-02T05:06:07Z"

		if state.runStatus == storage.RunCompleted {
			runOutput = []byte("output")
		}

		if state.runStatus == storage.RunFailed {
			runError = []byte("run-error")
		}
	}

	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, output_payload,
		terminal_node_id, error_payload, created_at, updated_at, completed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"legacy-run", "legacy-workflow", "legacy-definition", state.runStatus,
		[]byte("input"), runOutput, "legacy-node", runError,
		"2024-01-02T03:04:05Z", "2024-01-02T04:05:06Z", runCompleted)
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_owner, lease_generation, lease_expires_at,
		output_payload, error_payload, started_at, completed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"legacy-run", "legacy-node", "legacy-function", "legacy-signature", state.nodeStatus, 0,
		attempt, "2024-01-02T03:05:05Z", leaseOwner, leaseGeneration, leaseExpires,
		nodeOutput, nodeError, nodeStarted, nodeCompleted)
	require.NoError(t, err)
}

type priorRunRow struct {
	ID, Workflow, Definition, Status, Input, Output, Terminal, Failure string
	Created, Updated, Completed                                        string
	MaxAttempts, RetryPolicyVersion                                    int
	RetryBaseDelayNS, RetryMaxDelayNS                                  int64
}

func readPriorRun(t *testing.T, database *sql.DB, version int64) priorRunRow {
	t.Helper()

	var row priorRunRow

	columns := `id, workflow_name, definition_hash, status,
		quote(input_payload), quote(output_payload), terminal_node_id, quote(error_payload),
		quote(created_at), quote(updated_at), quote(completed_at)`
	destinations := []any{
		&row.ID, &row.Workflow, &row.Definition, &row.Status, &row.Input, &row.Output,
		&row.Terminal, &row.Failure, &row.Created, &row.Updated, &row.Completed,
	}

	if version >= 2 {
		columns += `, max_attempts, retry_base_delay_ns, retry_max_delay_ns, retry_policy_version`

		destinations = append(destinations,
			&row.MaxAttempts, &row.RetryBaseDelayNS, &row.RetryMaxDelayNS, &row.RetryPolicyVersion,
		)
	} else {
		row.MaxAttempts = 3
		row.RetryBaseDelayNS = 500000000
		row.RetryMaxDelayNS = 30000000000
		row.RetryPolicyVersion = 1
	}

	err := database.QueryRowContext(t.Context(),
		"SELECT "+columns+" FROM cord_runs WHERE id = 'legacy-run'",
	).Scan(destinations...)
	require.NoError(t, err)

	return row
}

type priorNodeRow struct {
	RunID, NodeID, Function, Signature, Status, Available, Owner, Expires string
	Output, Failure, Started, Completed                                   string
	Remaining, Attempt                                                    int
	Generation                                                            int64
}

func readPriorNode(t *testing.T, database *sql.DB) priorNodeRow {
	t.Helper()

	var row priorNodeRow

	err := database.QueryRowContext(t.Context(), `SELECT run_id, node_id, function_key, signature_hash,
		status, remaining_deps, attempt, quote(available_at), quote(lease_owner), lease_generation,
		quote(lease_expires_at), quote(output_payload), quote(error_payload), quote(started_at),
		quote(completed_at) FROM cord_nodes
		WHERE run_id = 'legacy-run' AND node_id = 'legacy-node'`).Scan(
		&row.RunID, &row.NodeID, &row.Function, &row.Signature, &row.Status,
		&row.Remaining, &row.Attempt, &row.Available, &row.Owner, &row.Generation,
		&row.Expires, &row.Output, &row.Failure, &row.Started, &row.Completed,
	)
	require.NoError(t, err)

	return row
}

func assertPriorLifecycleNull(t *testing.T, database *sql.DB) {
	t.Helper()

	var populated int

	err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM cord_runs r
		JOIN cord_nodes n ON n.run_id = r.id WHERE r.id = 'legacy-run' AND (
		r.lifecycle_version IS NOT NULL OR r.started_at IS NOT NULL OR
		r.terminal_reason IS NOT NULL OR r.terminal_runner_id IS NOT NULL OR
		n.lifecycle_version IS NOT NULL OR n.state_changed_at IS NOT NULL OR
		n.last_started_at IS NOT NULL OR n.last_runner_id IS NOT NULL OR
		n.terminal_reason IS NOT NULL)`).Scan(&populated)
	require.NoError(t, err)
	assert.Zero(t, populated, "migration must not invent lifecycle history")
}

func TestMigrateV5AddsNoIndexes(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	require.NoError(t, sqlite.MigrateToVersionForTest(t.Context(), database, 4))

	before := sqliteIndexes(t, database)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	assert.Equal(t, before, sqliteIndexes(t, database))
}

func sqliteIndexes(t *testing.T, database *sql.DB) []string {
	t.Helper()

	rows, err := database.QueryContext(t.Context(), `SELECT name FROM sqlite_schema
		WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%' ORDER BY name`)
	require.NoError(t, err)

	defer func() { require.NoError(t, rows.Close()) }()

	var indexes []string

	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		indexes = append(indexes, name)
	}

	require.NoError(t, rows.Err())

	return indexes
}

func TestMigrateUpgradesV3RowsAndExecutesThem(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	prepareV3Schema(t, database)

	var err error

	const (
		runID         = storage.RunID("pre-async-sqlite-run")
		nodeID        = storage.NodeID("terminal")
		functionKey   = "pre.async.sqlite"
		signatureHash = "pre-async-signature"
	)

	createdAt := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC).Format(time.RFC3339Nano)
	_, err = database.ExecContext(t.Context(), `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, terminal_node_id,
		created_at, updated_at, max_attempts, retry_base_delay_ns,
		retry_max_delay_ns, retry_policy_version
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		runID, "pre-async-sqlite", "definition", storage.RunRunning, []byte("41"), nodeID,
		createdAt, createdAt, 3, time.Millisecond.Nanoseconds(), time.Second.Nanoseconds(), 1)
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_generation
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		runID, nodeID, functionKey, signatureHash, storage.NodeReady, 0, 0, createdAt, 0)
	require.NoError(t, err)

	err = sqlite.Verify(t.Context(), database)
	require.ErrorIs(t, err, storage.ErrSchemaOutdated)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	require.NoError(t, sqlite.Verify(t.Context(), database))

	store, err := sqlite.New(database)
	require.NoError(t, err)
	claim, claimed, err := store.ClaimReadyNodeForFunctions(t.Context(), "migration-worker", time.Minute,
		[]storage.FunctionRegistration{{Key: functionKey, Signature: signatureHash}})
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, runID, claim.RunID)

	inputs, err := store.LoadNodeInputs(t.Context(), claim.RunID, claim.NodeID)
	require.NoError(t, err)
	assert.Equal(t, []storage.EncodedPayload{[]byte("41")}, inputs)
	accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("42"))
	require.NoError(t, err)
	require.True(t, accepted)

	result, err := store.GetRunResult(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, storage.RunCompleted, result.Status)
	assert.Equal(t, storage.EncodedPayload("42"), result.Output)
	assert.Equal(t, "pre-async-sqlite", result.WorkflowName)
	assert.Equal(t, signatureHash, result.TerminalSignatureHash)

	var (
		key, fingerprint            sql.NullString
		runLifecycle, nodeLifecycle sql.NullInt64
	)

	err = database.QueryRowContext(t.Context(), `SELECT idempotency_key, submission_fingerprint,
		lifecycle_version FROM cord_runs WHERE id = ?`, runID).Scan(&key, &fingerprint, &runLifecycle)
	require.NoError(t, err)
	require.NoError(t, database.QueryRowContext(t.Context(),
		`SELECT lifecycle_version FROM cord_nodes WHERE run_id = ? AND node_id = ?`, runID, nodeID,
	).Scan(&nodeLifecycle))
	assert.False(t, key.Valid)
	assert.False(t, fingerprint.Valid)
	require.True(t, runLifecycle.Valid)
	require.True(t, nodeLifecycle.Valid)
	assert.EqualValues(t, 1, runLifecycle.Int64)
	assert.EqualValues(t, 1, nodeLifecycle.Int64)
}

func prepareV3Schema(t *testing.T, database *sql.DB) {
	t.Helper()

	require.NoError(t, sqlite.Migrate(t.Context(), database))

	_, err := database.ExecContext(t.Context(), "DROP INDEX cord_runs_workflow_name_idempotency_key_idx")
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), "ALTER TABLE cord_runs DROP COLUMN idempotency_key")
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), "ALTER TABLE cord_runs DROP COLUMN submission_fingerprint")
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(),
		"DELETE FROM cord_schema_migrations WHERE version_id >= 4")
	require.NoError(t, err)
}

func TestVerifyTreatsLatestRolledBackMigrationAsPreviousVersion(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_schema_migrations
		(version_id, is_applied, tstamp) VALUES (6, 0, datetime('now'))`)
	require.NoError(t, err)
	require.NoError(t, sqlite.Verify(t.Context(), database))
}

func TestVerifyReturnsDatabaseInspectionErrors(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	require.NoError(t, database.Close())
	err := sqlite.Verify(t.Context(), database)
	require.Error(t, err)
	require.NotErrorIs(t, err, storage.ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "inspect sqlite schema")
}
