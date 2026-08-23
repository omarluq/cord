package sqlstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
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

type probeResponse struct {
	value driver.Value
	err   error
}

type probeConnector struct {
	responses map[string]probeResponse
}

func (connector probeConnector) Connect(context.Context) (driver.Conn, error) {
	return probeConnection(connector), nil
}

func (probeConnector) Driver() driver.Driver {
	return probeDriver{}
}

type probeDriver struct{}

func (probeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("probe driver requires its connector")
}

type probeConnection struct {
	responses map[string]probeResponse
}

func (probeConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (probeConnection) Close() error { return nil }

func (probeConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (connection probeConnection) QueryContext(
	_ context.Context,
	query string,
	_ []driver.NamedValue,
) (driver.Rows, error) {
	response, ok := connection.responses[query]
	if !ok {
		return nil, fmt.Errorf("unexpected query %q", query)
	}

	if response.err != nil {
		return nil, response.err
	}

	return &probeRows{value: response.value, read: false}, nil
}

type probeRows struct {
	value driver.Value
	read  bool
}

func (*probeRows) Columns() []string { return []string{"capability"} }
func (*probeRows) Close() error      { return nil }

func (rows *probeRows) Next(values []driver.Value) error {
	if rows.read {
		return io.EOF
	}

	values[0] = rows.value
	rows.read = true

	return nil
}

func openProbeDatabase(t *testing.T, responses map[string]probeResponse) *sql.DB {
	t.Helper()

	database := sql.OpenDB(probeConnector{responses: responses})

	t.Cleanup(func() {
		require.NoError(t, database.Close())
	})

	return database
}
