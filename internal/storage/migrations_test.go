package storage_test

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	"github.com/omarluq/cord/internal/storage"
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

			results <- storage.Migrate(t.Context(), database)
		})
	}

	close(start)
	waitGroup.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}

	require.NoError(t, storage.Verify(t.Context(), first))
	require.NoError(t, storage.Verify(t.Context(), second))

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
		err := storage.Verify(t.Context(), database)
		require.ErrorIs(t, err, storage.ErrSchemaOutdated)
	})

	t.Run("current", func(t *testing.T) {
		t.Parallel()

		database := openDatabase(t, true)
		require.NoError(t, storage.Migrate(t.Context(), database))
		require.NoError(t, storage.Verify(t.Context(), database))
	})

	t.Run("newer", func(t *testing.T) {
		t.Parallel()

		database := openDatabase(t, true)
		require.NoError(t, storage.Migrate(t.Context(), database))
		_, err := database.ExecContext(t.Context(), `INSERT INTO cord_schema_migrations
			(version_id, is_applied, tstamp) VALUES (3, 1, datetime('now'))`)
		require.NoError(t, err)
		err = storage.Verify(t.Context(), database)
		require.ErrorIs(t, err, storage.ErrSchemaNewer)
		assert.Contains(t, err.Error(), "current=3 required=2")
	})
}

func TestVerifyTreatsLatestRolledBackMigrationAsPreviousVersion(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	require.NoError(t, storage.Migrate(t.Context(), database))
	_, err := database.ExecContext(t.Context(), `INSERT INTO cord_schema_migrations
		(version_id, is_applied, tstamp) VALUES (3, 0, datetime('now'))`)
	require.NoError(t, err)
	require.NoError(t, storage.Verify(t.Context(), database))
}

func TestVerifyReturnsDatabaseInspectionErrors(t *testing.T) {
	t.Parallel()

	database := openDatabase(t, true)
	require.NoError(t, database.Close())
	err := storage.Verify(t.Context(), database)
	require.Error(t, err)
	require.NotErrorIs(t, err, storage.ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "inspect sqlite schema")
}
