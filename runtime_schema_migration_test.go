package cord_test

import (
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_MigratesOldSchema(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, runtime.Close())

	for _, statement := range []string{
		"DROP TABLE cord_edges",
		"DROP TABLE cord_nodes",
		"DROP TABLE cord_runs",
		"DELETE FROM cord_schema_migrations",
		"INSERT INTO cord_schema_migrations (version_id, is_applied) VALUES (0, 1)",
	} {
		_, err = database.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}

	runtime, err = cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, runtime.Close())

	for _, table := range []string{"cord_runs", "cord_nodes", "cord_edges"} {
		assert.True(t, sqliteTableExists(t, database, table), table)
	}

	var applied int

	err = database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_schema_migrations WHERE version_id = 1 AND is_applied = 1",
	).Scan(&applied)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
}

func TestNew_FailedMigrationRollsBack(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	_, err := database.ExecContext(t.Context(), "CREATE TABLE cord_nodes (id TEXT PRIMARY KEY)")
	require.NoError(t, err)

	runtime, err := cord.New(t.Context(), database)

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrMigrationFailed)
	assert.False(t, sqliteTableExists(t, database, "cord_runs"))
	assert.False(t, sqliteTableExists(t, database, "cord_edges"))
	assert.True(t, sqliteTableExists(t, database, "cord_nodes"))

	var applied int

	err = database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_schema_migrations WHERE version_id = 1 AND is_applied = 1",
	).Scan(&applied)
	require.NoError(t, err)
	assert.Zero(t, applied)
}

func TestNew_RejectsNewerSchema(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, runtime.Close())

	_, err = database.ExecContext(
		t.Context(),
		"INSERT INTO cord_schema_migrations (version_id, is_applied) VALUES (6, 1)",
	)
	require.NoError(t, err)

	runtime, err = cord.New(t.Context(), database)

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrSchemaNewer)
	require.ErrorContains(t, err, "current=")
	require.ErrorContains(t, err, "required=")
}

func TestNew_ReportsDatabaseFailure(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	require.NoError(t, database.Close())

	runtime, err := cord.New(t.Context(), database)

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrMigrationFailed)
}
