package sqlite_test

import (
	"database/sql"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

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
