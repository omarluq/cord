package exampledb_test

import (
	"context"
	"testing"

	"github.com/omarluq/cord/internal/exampledb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenSQLiteConfiguresUsableIsolatedDatabase(t *testing.T) {
	t.Parallel()

	database, err := exampledb.OpenSQLite(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	stats := database.Stats()
	assert.Equal(t, 1, stats.MaxOpenConnections)

	_, err = database.ExecContext(t.Context(), `
		CREATE TABLE parent (id INTEGER PRIMARY KEY);
		CREATE TABLE child (parent_id INTEGER REFERENCES parent(id));
	`)
	require.NoError(t, err)

	_, err = database.ExecContext(t.Context(), "INSERT INTO child(parent_id) VALUES (99)")
	require.Error(t, err, "foreign key enforcement must be enabled")
}

func TestOpenSQLiteHonorsCallerCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	database, err := exampledb.OpenSQLite(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	require.ErrorIs(t, database.PingContext(ctx), context.Canceled)
}

func TestOpenPostgresRequiresDSN(t *testing.T) {
	t.Setenv("CORD_POSTGRES_DSN", "")

	database, err := exampledb.OpenPostgres(t.Context())

	assert.Nil(t, database)
	require.EqualError(t, err, "CORD_POSTGRES_DSN is required")
}

func TestOpenPostgresReportsFailedHealthCheck(t *testing.T) {
	t.Setenv("CORD_POSTGRES_DSN", "postgres://127.0.0.1:1/cord?connect_timeout=1")

	database, err := exampledb.OpenPostgres(t.Context())

	assert.Nil(t, database)
	require.ErrorContains(t, err, "ping PostgreSQL")
}
