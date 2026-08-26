package postgres_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postgresAddOne(_ context.Context, value int) (int, error)     { return value + 1, nil }
func postgresDouble(_ context.Context, value int) (int, error)     { return value * 2, nil }
func postgresJoin(_ context.Context, left, right int) (int, error) { return left + right, nil }

func postgresRetryUntilFileExists(_ context.Context, path string) (string, error) {
	if _, err := os.Stat(path); err != nil {
		return path, fmt.Errorf("wait for resume marker: %w", err)
	}

	return path, nil
}

// TestCordNewPublicWorkflowsAndCallerOwnership exercises Cord's public PostgreSQL workflow API.
func TestCordNewPublicWorkflowsAndCallerOwnership(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	cordRuntime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)

	linear, err := cordRuntime.From("postgres-linear", postgresAddOne).Then(postgresDouble).Run(t.Context(), 4)
	require.NoError(t, err)
	assert.Equal(t, 10, linear)

	root := cordRuntime.From("postgres-join", postgresAddOne)
	joined, err := cord.Join(root.Then(postgresDouble), root.Then(postgresAddOne)).
		Then(postgresJoin).
		Run(t.Context(), 3)
	require.NoError(t, err)
	assert.Equal(t, 13, joined)

	require.NoError(t, cordRuntime.Close())
	require.NoError(t, database.PingContext(t.Context()), "Cord must not close its caller-owned database")
}

type wrappedPGXConnector struct {
	connector driver.Connector
}

// Connect delegates to the wrapped pgx connector.
func (connector wrappedPGXConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connection, err := connector.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect wrapped pgx driver: %w", err)
	}

	return connection, nil
}

// Driver returns a wrapped pgx driver.
func (connector wrappedPGXConnector) Driver() driver.Driver {
	return wrappedPGXDriver{driver: connector.connector.Driver()}
}

type wrappedPGXDriver struct {
	driver driver.Driver
}

// Open delegates to the wrapped pgx driver.
func (wrapped wrappedPGXDriver) Open(name string) (driver.Conn, error) {
	connection, err := wrapped.driver.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open wrapped pgx driver: %w", err)
	}

	return connection, nil
}

// TestCordNewDetectsPostgresThroughWrappedPGXConnector verifies wrapped pgx driver detection.
func TestCordNewDetectsPostgresThroughWrappedPGXConnector(t *testing.T) {
	t.Parallel()

	config, err := pgx.ParseConfig(startPostgres(t))
	require.NoError(t, err)

	pgxConnector := stdlib.GetConnector(*config)
	database := sql.OpenDB(wrappedPGXConnector{connector: pgxConnector})
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(8)
	t.Cleanup(func() { assert.NoError(t, database.Close()) })

	require.NotEqual(t, reflect.TypeOf(pgxConnector.Driver()), reflect.TypeOf(database.Driver()))

	cordRuntime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cordRuntime.Close()) })

	result, err := cordRuntime.From("postgres-wrapped-pgx", postgresAddOne).Run(t.Context(), 4)
	require.NoError(t, err)
	assert.Equal(t, 5, result)
}
