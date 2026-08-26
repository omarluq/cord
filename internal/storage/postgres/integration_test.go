package postgres_test

import (
	"context"
	"database/sql"
	"log"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	postgresImage          = "postgres:16.4-alpine"
	postgresMaxConnections = "256"
	fixtureTimeout         = 2 * time.Minute
	operationTimeout       = 30 * time.Second
)

// TestMain provisions the shared PostgreSQL fixture when no external test database is configured.
func TestMain(m *testing.M) {
	os.Exit(runPostgresTests(m))
}

func runPostgresTests(m *testing.M) (exitCode int) {
	if os.Getenv("CORD_POSTGRES_DSN") != "" {
		return m.Run()
	}

	ctx, cancel := context.WithTimeout(context.Background(), fixtureTimeout)
	defer cancel()

	container, err := postgrescontainer.Run(
		ctx,
		postgresImage,
		postgrescontainer.WithDatabase("cord"),
		postgrescontainer.WithUsername("cord"),
		postgrescontainer.WithPassword("cord"),
		postgrescontainer.BasicWaitStrategies(),
		testcontainers.WithCmdArgs("-c", "max_connections="+postgresMaxConnections),
	)
	if err != nil {
		log.Printf("start PostgreSQL test container: %v", err)

		return 1
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cleanupCancel()

		if terminateErr := container.Terminate(cleanupCtx); terminateErr != nil {
			log.Printf("terminate PostgreSQL test container: %v", terminateErr)

			if exitCode == 0 {
				exitCode = 1
			}
		}
	}()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("resolve PostgreSQL test connection: %v", err)

		return 1
	}

	if err = os.Setenv("CORD_POSTGRES_DSN", dsn); err != nil {
		log.Printf("publish PostgreSQL test connection: %v", err)

		return 1
	}

	return m.Run()
}

func startPostgres(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("CORD_POSTGRES_DSN")
	require.NotEmpty(t, dsn, "PostgreSQL test connection was not initialized")

	return isolatePostgresSchema(t, dsn)
}

func isolatePostgresSchema(t *testing.T, dsn string) string {
	t.Helper()

	schema := "cord_test_" + strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()

		_, dropErr := admin.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		assert.NoError(t, dropErr, "drop isolated PostgreSQL schema")
		assert.NoError(t, admin.Close(), "close PostgreSQL schema administrator")
	})

	if parsed, parseErr := url.Parse(dsn); parseErr == nil && parsed.Scheme != "" {
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()

		return parsed.String()
	}

	return dsn + " search_path=" + schema
}

func openPostgres(tb testing.TB, dsn string) *sql.DB {
	tb.Helper()

	database, err := sql.Open("pgx", dsn)
	if err != nil {
		tb.Fatalf("open PostgreSQL: %v", err)
	}

	database.SetMaxOpenConns(4)
	database.SetMaxIdleConns(2)

	if err = database.PingContext(context.Background()); err != nil {
		closeErr := database.Close()

		tb.Fatalf("ping PostgreSQL: %v (close after failure: %v)", err, closeErr)
	}

	const resetQuery = `DROP TABLE IF EXISTS cord_edges, cord_nodes, cord_runs, cord_schema_migrations CASCADE`
	if _, err = database.ExecContext(context.Background(), resetQuery); err != nil {
		closeErr := database.Close()

		tb.Fatalf("reset PostgreSQL fixture: %v (close after failure: %v)", err, closeErr)
	}

	tb.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			tb.Errorf("close PostgreSQL: %v", closeErr)
		}
	})

	return database
}

func openPostgresPool(tb testing.TB, dsn string) *sql.DB {
	tb.Helper()

	database, err := sql.Open("pgx", dsn)
	require.NoError(tb, err)
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(4)
	require.NoError(tb, database.PingContext(context.Background()))
	tb.Cleanup(func() { assert.NoError(tb, database.Close()) })

	return database
}
