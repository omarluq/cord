package storage_test

import (
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
