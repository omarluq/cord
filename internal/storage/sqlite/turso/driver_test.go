package turso_test

import (
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage/conformance"
	"github.com/omarluq/cord/internal/storage/sqlite"
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
	database.SetMaxOpenConns(1)
	require.NoError(tb, database.PingContext(tb.Context()))
	require.NoError(tb, enableForeignKeys(tb, database))
	tb.Cleanup(func() { require.NoError(tb, database.Close()) })

	return database
}

func enableForeignKeys(tb testing.TB, database *sql.DB) error {
	tb.Helper()

	if _, err := database.ExecContext(tb.Context(), "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign keys: %w", err)
	}

	return nil
}
