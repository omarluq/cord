package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage/sqlstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestDetect(t *testing.T) {
	t.Parallel()

	sqliteProbeErr := errors.New("not SQLite")
	postgresProbeErr := errors.New("not PostgreSQL")

	tests := []struct {
		responses map[string]probeResponse
		name      string
		wantError string
		want      sqlstore.Dialect
	}{
		{
			name: "SQLite",
			responses: map[string]probeResponse{
				sqlstore.SQLiteCapabilityProbe: {value: "3.50.0", err: nil},
			},
			wantError: "",
			want:      sqlstore.DialectSQLite,
		},
		{
			name: "PostgreSQL",
			responses: map[string]probeResponse{
				sqlstore.SQLiteCapabilityProbe:   {value: nil, err: sqliteProbeErr},
				sqlstore.PostgresCapabilityProbe: {value: int64(170002), err: nil},
			},
			wantError: "",
			want:      sqlstore.DialectPostgres,
		},
		{
			name: "unsupported backend",
			responses: map[string]probeResponse{
				sqlstore.SQLiteCapabilityProbe:   {value: nil, err: sqliteProbeErr},
				sqlstore.PostgresCapabilityProbe: {value: nil, err: postgresProbeErr},
			},
			wantError: "detect SQL storage backend",
			want:      sqlstore.DialectSQLite,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database := openProbeDatabase(t, testCase.responses)
			got, err := sqlstore.Detect(t.Context(), database)

			if testCase.wantError != "" {
				require.ErrorContains(t, err, testCase.wantError)
				require.ErrorIs(t, err, sqliteProbeErr)
				require.ErrorIs(t, err, postgresProbeErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, testCase.want, got)
		})
	}
}

func TestDetectTreatsSQLiteBusyAsSQLite(t *testing.T) {
	t.Parallel()

	database, err := sql.Open(
		"sqlite",
		"file:"+t.TempDir()+"/busy.db?_pragma=busy_timeout(0)&_pragma=locking_mode(EXCLUSIVE)",
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	connection, err := database.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })

	_, err = connection.ExecContext(t.Context(), "CREATE TABLE lock_holder (id INTEGER)")
	require.NoError(t, err)

	transaction, err := connection.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			t.Errorf("rollback lock transaction: %v", rollbackErr)
		}
	})

	_, err = transaction.ExecContext(t.Context(), "INSERT INTO lock_holder VALUES (1)")
	require.NoError(t, err)

	detectionCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err = sqlstore.Detect(detectionCtx, database)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotContains(t, err.Error(), "PostgreSQL capability probe")
}

func TestNewBootstrapsSQLite(t *testing.T) {
	t.Parallel()

	database, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(1)")
	require.NoError(t, err)
	database.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	backend, err := sqlstore.New(t.Context(), database)
	require.NoError(t, err)
	require.NotNil(t, backend)

	var migrationCount int
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_schema_migrations WHERE is_applied = 1",
	).Scan(&migrationCount))
	assert.Positive(t, migrationCount)

	var nodeTable string
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'cord_nodes'",
	).Scan(&nodeTable))
	assert.Equal(t, "cord_nodes", nodeTable)
}

func TestDetectCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	database := openProbeDatabase(t, map[string]probeResponse{})

	_, err := sqlstore.Detect(ctx, database)

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "detect SQL storage backend")
}

func TestNewDispatchesPostgres(t *testing.T) {
	t.Parallel()

	database := openProbeDatabase(t, map[string]probeResponse{
		sqlstore.SQLiteCapabilityProbe:   {value: nil, err: errors.New("not SQLite")},
		sqlstore.PostgresCapabilityProbe: {value: int64(170002), err: nil},
	})

	backend, err := sqlstore.New(t.Context(), database)

	assert.Nil(t, backend)
	require.ErrorContains(t, err, "migrate PostgreSQL storage")
	require.ErrorContains(t, err, "transactions are not supported")
}
