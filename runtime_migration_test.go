package cord_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_CreatesRuntime(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, mustRuntime(t))
}

func TestNew_CanceledMigrationContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	runtime, err := cord.New(ctx, openSQLite(t))

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, context.Canceled)
}

func TestNew_RejectsMultipleOptions(t *testing.T) {
	t.Parallel()

	runtime, err := cord.New(t.Context(), openSQLite(t), cord.Options{}, cord.Options{})

	assert.Nil(t, runtime)
	require.ErrorContains(t, err, "at most one options")
}

func TestNew_RejectsNilDatabase(t *testing.T) {
	t.Parallel()

	runtime, err := cord.New(t.Context(), nil)

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrMigrationFailed)
}

func TestNew_MigratesAndLeavesDatabaseOpen(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NotNil(t, runtime)

	for _, table := range []string{"cord_schema_migrations", "cord_runs", "cord_nodes", "cord_edges"} {
		assert.True(t, sqliteTableExists(t, database, table), table)
	}

	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
	require.NoError(t, database.PingContext(t.Context()))
}

func TestNew_IsRepeatable(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)

	first, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	second, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, second.Close())

	var applied int

	err = database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_schema_migrations WHERE version_id = 1 AND is_applied = 1",
	).Scan(&applied)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
}

func TestNew_ConcurrentCalls(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "concurrent.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"

	const constructors = 8

	var waitGroup sync.WaitGroup

	results := make(chan error, constructors)

	for range constructors {
		waitGroup.Go(func() {
			database, err := sql.Open("sqlite", dsn)
			if err != nil {
				results <- err

				return
			}

			runtime, err := cord.New(t.Context(), database)
			if err == nil {
				err = runtime.Close()
			}

			if closeErr := database.Close(); err == nil {
				err = closeErr
			}

			results <- err
		})
	}

	waitGroup.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}
}
