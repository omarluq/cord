package remotelock_test

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
	"path/filepath"
	"testing"
)

func openDatabase(t *testing.T) (*sql.DB, *sql.Conn) {
	t.Helper()

	database := openPath(t, filepath.Join(t.TempDir(), "remote-lock.db"))
	connection, err := database.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := connection.Close()
		if err != nil && !errors.Is(err, sql.ErrConnDone) {
			require.NoError(t, err)
		}
	})

	return database, connection
}

func openPath(t *testing.T, path string) *sql.DB {
	t.Helper()

	database, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", path))
	require.NoError(t, err)
	database.SetMaxOpenConns(4)
	t.Cleanup(func() {
		if err := database.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			require.NoError(t, err)
		}
	})

	return database
}

func createLockTable(t *testing.T, database *sql.DB) {
	t.Helper()

	_, err := database.ExecContext(t.Context(), `CREATE TABLE cord_migration_lock (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		owner TEXT NOT NULL,
		expires_at INTEGER NOT NULL
	)`)
	require.NoError(t, err)
}

func lockRowsIfTableExists(t *testing.T, database *sql.DB) int {
	t.Helper()

	var count int
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'table' AND name = 'cord_migration_lock'`).Scan(&count))

	return count
}

func lockRows(t *testing.T, database *sql.DB) int {
	t.Helper()

	var count int
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM cord_migration_lock").Scan(&count))

	return count
}
