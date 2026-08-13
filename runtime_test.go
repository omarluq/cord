package cord_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestNewDurable_ValidatesConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target error
		name   string
		config cord.DurableConfig
	}{
		{
			name:   "nil database",
			config: cord.DurableConfig{},
			target: cord.ErrMigrationFailed,
		},
		{
			name: "missing dialect",
			config: cord.DurableConfig{
				DB: openSQLite(t),
			},
			target: cord.ErrUnsupportedDialect,
		},
		{
			name: "unknown migration mode",
			config: cord.DurableConfig{
				DB:            openSQLite(t),
				Dialect:       cord.DialectSQLite,
				MigrationMode: cord.MigrationMode(99),
			},
			target: cord.ErrMigrationFailed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			runtime, err := cord.NewDurable(test.config)

			assert.Nil(t, runtime)
			require.ErrorIs(t, err, test.target)
		})
	}
}

func TestNewDurable_VerifyOnlyDoesNotCreateSchema(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.NewDurable(cord.DurableConfig{
		DB:      database,
		Dialect: cord.DialectSQLite,
	})

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrSchemaOutdated)
	assert.False(t, sqliteTableExists(t, database, "cord_schema_migrations"))
}

func TestNewDurable_MigratesAndLeavesDatabaseOpen(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.NewDurable(cord.DurableConfig{
		DB:            database,
		Dialect:       cord.DialectSQLite,
		MigrationMode: cord.MigrationOnInitialization,
	})
	require.NoError(t, err)
	require.NotNil(t, runtime)

	for _, table := range []string{"cord_schema_migrations", "cord_runs", "cord_nodes", "cord_edges"} {
		assert.True(t, sqliteTableExists(t, database, table), table)
	}

	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
	require.NoError(t, database.PingContext(t.Context()))
}

func TestMigrate_IsRepeatable(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)

	require.NoError(t, cord.Migrate(t.Context(), database, cord.DialectSQLite))
	require.NoError(t, cord.Migrate(t.Context(), database, cord.DialectSQLite))

	var applied int

	err := database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_schema_migrations WHERE version_id = 1 AND is_applied = 1",
	).Scan(&applied)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
}

func TestMigrate_ConcurrentCalls(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)

	const calls = 8

	var waitGroup sync.WaitGroup

	errorsChannel := make(chan error, calls)
	for range calls {
		waitGroup.Go(func() {
			errorsChannel <- cord.Migrate(t.Context(), database, cord.DialectSQLite)
		})
	}

	waitGroup.Wait()
	close(errorsChannel)

	for err := range errorsChannel {
		require.NoError(t, err)
	}
}

func TestNewDurable_RejectsOldAndNewerSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target error
		mutate func(*testing.T, *sql.DB)
		name   string
	}{
		{
			name: "old",
			mutate: func(t *testing.T, database *sql.DB) {
				t.Helper()
				_, err := database.ExecContext(t.Context(), "DELETE FROM cord_schema_migrations WHERE version_id = 1")
				require.NoError(t, err)
			},
			target: cord.ErrSchemaOutdated,
		},
		{
			name: "newer",
			mutate: func(t *testing.T, database *sql.DB) {
				t.Helper()
				_, err := database.ExecContext(
					t.Context(),
					"INSERT INTO cord_schema_migrations (version_id, is_applied) VALUES (2, 1)",
				)
				require.NoError(t, err)
			},
			target: cord.ErrSchemaNewer,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database := openSQLite(t)
			require.NoError(t, cord.Migrate(t.Context(), database, cord.DialectSQLite))
			test.mutate(t, database)

			runtime, err := cord.NewDurable(cord.DurableConfig{DB: database, Dialect: cord.DialectSQLite})

			assert.Nil(t, runtime)
			require.ErrorIs(t, err, test.target)
			assert.Contains(t, err.Error(), "current=")
			assert.Contains(t, err.Error(), "required=")
		})
	}
}

func TestMigrate_RejectsUnsupportedDialect(t *testing.T) {
	t.Parallel()

	err := cord.Migrate(t.Context(), openSQLite(t), cord.DialectPostgres)

	require.ErrorIs(t, err, cord.ErrUnsupportedDialect)
}

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()

	dsn := filepath.Join(t.TempDir(), "cord.db")
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, database.Close())
	})

	_, err = database.ExecContext(t.Context(), "PRAGMA foreign_keys = ON")
	require.NoError(t, err)

	return database
}

func sqliteTableExists(t *testing.T, database *sql.DB, table string) bool {
	t.Helper()

	var one int

	err := database.QueryRowContext(
		t.Context(),
		"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}

	require.NoError(t, err)

	return true
}
