package turso_test

import (
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage/conformance"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/omarluq/cord/internal/storage/sqlite/remotelock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	// Register the libSQL database/sql driver used by the conformance tests.
	_ "github.com/tursodatabase/go-libsql"
)

const (
	libSQLImage = "ghcr.io/tursodatabase/libsql-server:v0.24.32"
	libSQLPort  = "8080/tcp"
)

// TestDriverConformance verifies Cord's storage behavior against remote libSQL.
func TestDriverConformance(t *testing.T) {
	t.Parallel()

	databaseURL := startLibSQL(t)

	conformance.Run(t, conformance.Driver{
		DataSource:          nil,
		Name:                "",
		Open:                libSQLOpener(databaseURL),
		SkipWriteContention: true,
	})
}

// TestRemoteMigrationRejectsSingleConnectionPool verifies remote locking fails before it can self-block.
func TestRemoteMigrationRejectsSingleConnectionPool(t *testing.T) {
	t.Parallel()

	database := openLibSQL(t, startLibSQL(t))
	database.SetMaxOpenConns(1)

	err := sqlite.Migrate(t.Context(), database)
	require.ErrorIs(t, err, remotelock.ErrInsufficientPoolCapacity)
	require.Zero(t, database.Stats().WaitCount)
}

// TestConcurrentMigrations verifies remote libSQL serializes concurrent migrations.
func TestConcurrentMigrations(t *testing.T) {
	t.Parallel()

	databaseURL := startLibSQL(t)
	first := openLibSQL(t, databaseURL)
	second := openLibSQL(t, databaseURL)

	results := make(chan error, 2)
	go func() { results <- sqlite.Migrate(t.Context(), first) }()
	go func() { results <- sqlite.Migrate(t.Context(), second) }()

	require.NoError(t, <-results)
	require.NoError(t, <-results)

	rows, err := first.QueryContext(t.Context(), `SELECT version_id, COUNT(*)
		FROM cord_schema_migrations WHERE is_applied = 1 AND version_id > 0
		GROUP BY version_id ORDER BY version_id`)
	require.NoError(t, err)

	defer func() { require.NoError(t, rows.Close()) }()

	var versions []int64

	for rows.Next() {
		var (
			version int64
			count   int
		)
		require.NoError(t, rows.Scan(&version, &count))
		require.Equal(t, 1, count)

		versions = append(versions, version)
	}

	require.NoError(t, rows.Err())
	require.Equal(t, []int64{1, 2}, versions)
}

// TestAbandonedMigrationLock verifies an expired remote lock can be recovered.
func TestAbandonedMigrationLock(t *testing.T) {
	t.Parallel()

	database := openLibSQL(t, startLibSQL(t))
	_, err := database.ExecContext(t.Context(), `CREATE TABLE cord_migration_lock (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		owner TEXT NOT NULL,
		expires_at INTEGER NOT NULL
	)`)
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(),
		"INSERT INTO cord_migration_lock (id, owner, expires_at) VALUES (1, 'abandoned', unixepoch() - 1)")
	require.NoError(t, err)

	require.NoError(t, sqlite.Migrate(t.Context(), database))

	var locks int
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM cord_migration_lock").Scan(&locks))
	require.Zero(t, locks)
}

func startLibSQL(t *testing.T) string {
	t.Helper()

	container, err := testcontainers.Run(
		t.Context(),
		libSQLImage,
		testcontainers.WithEnv(map[string]string{"SQLD_NODE": "primary"}),
		testcontainers.WithExposedPorts(libSQLPort),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort(libSQLPort).WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, container)
	require.NoError(t, err)

	host, err := container.Host(t.Context())
	require.NoError(t, err)

	port, err := container.MappedPort(t.Context(), libSQLPort)
	require.NoError(t, err)

	return "http://" + net.JoinHostPort(host, port.Port())
}

func libSQLOpener(databaseURL string) func(testing.TB) *sql.DB {
	return func(tb testing.TB) *sql.DB {
		tb.Helper()

		return openLibSQL(tb, databaseURL)
	}
}

func openLibSQL(tb testing.TB, databaseURL string) *sql.DB {
	tb.Helper()

	database, err := sql.Open("libsql", databaseURL)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, database.Close()) })

	database.SetMaxOpenConns(2)
	require.NoError(tb, database.PingContext(tb.Context()))
	require.NoError(tb, enableForeignKeys(tb, database))

	return database
}

func enableForeignKeys(tb testing.TB, database *sql.DB) error {
	tb.Helper()

	if _, err := database.ExecContext(tb.Context(), "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign keys: %w", err)
	}

	return nil
}
