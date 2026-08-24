package sqlite_test

import (
	"database/sql"
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
	assert.Equal(t, []int64{1, 2, 3, 4}, versions)
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
			(version_id, is_applied, tstamp) VALUES (5, 1, datetime('now'))`)
		require.NoError(t, err)
		err = sqlite.Verify(t.Context(), database)
		require.ErrorIs(t, err, storage.ErrSchemaNewer)
		assert.Contains(t, err.Error(), "current=5 required=4")
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

	return append(cases, identityIncompatibleSchemaCases()...)
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

func TestMigrateUpgradesV3RowsAndExecutesThem(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	require.NoError(t, sqlite.Migrate(t.Context(), database))

	_, err := database.ExecContext(t.Context(), "DROP INDEX cord_runs_workflow_name_idempotency_key_idx")
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), "ALTER TABLE cord_runs DROP COLUMN idempotency_key")
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), "ALTER TABLE cord_runs DROP COLUMN submission_fingerprint")
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(),
		"DELETE FROM cord_schema_migrations WHERE version_id = 4")
	require.NoError(t, err)

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

	var key, fingerprint sql.NullString

	err = database.QueryRowContext(t.Context(), `SELECT idempotency_key, submission_fingerprint
		FROM cord_runs WHERE id = ?`, runID).Scan(&key, &fingerprint)
	require.NoError(t, err)
	assert.False(t, key.Valid)
	assert.False(t, fingerprint.Valid)
}

func TestVerifyTreatsLatestRolledBackMigrationAsPreviousVersion(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_schema_migrations
		(version_id, is_applied, tstamp) VALUES (5, 0, datetime('now'))`)
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
