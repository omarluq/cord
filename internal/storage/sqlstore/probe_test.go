package sqlstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

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

	t.Cleanup(func() { require.NoError(t, database.Close()) })

	return database
}
