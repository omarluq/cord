package sqlite_test

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

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
	assert.Equal(t, []int64{1, 2}, versions)
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
			(version_id, is_applied, tstamp) VALUES (3, 1, datetime('now'))`)
		require.NoError(t, err)
		err = sqlite.Verify(t.Context(), database)
		require.ErrorIs(t, err, storage.ErrSchemaNewer)
		assert.Contains(t, err.Error(), "current=3 required=2")
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
			name:       "missing index",
			statements: []string{`DROP INDEX cord_nodes_lease_expires_at_idx`},
			wantError:  `index "cord_nodes_lease_expires_at_idx" is missing`,
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
			},
			wantError: `table "cord_edges" is missing foreign key`,
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

func TestVerifyTreatsLatestRolledBackMigrationAsPreviousVersion(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_schema_migrations
		(version_id, is_applied, tstamp) VALUES (3, 0, datetime('now'))`)
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
