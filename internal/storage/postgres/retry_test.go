package postgres_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	postgresstore "github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err       error
		name      string
		retryable bool
	}{
		{name: "serialization failure", err: &pgconn.PgError{Code: "40001"}, retryable: true},
		{name: "deadlock", err: fmt.Errorf("statement: %w", &pgconn.PgError{Code: "40P01"}), retryable: true},
		{name: "lock unavailable", err: &pgconn.PgError{Code: "55P03"}, retryable: false},
		{name: "connection failure", err: &pgconn.PgError{Code: "08006"}, retryable: false},
		{name: "constraint violation", err: &pgconn.PgError{Code: "23505"}, retryable: false},
		{name: "ordinary error", err: errors.New("failed"), retryable: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.retryable, postgresstore.RetryableForTest(test.err))
		})
	}
}

func TestRetryDoesNotReplayNontransientError(t *testing.T) {
	t.Parallel()

	calls := 0
	err := postgresstore.RunOperationForTest(t.Context(), "test operation", func() error {
		calls++

		return &pgconn.PgError{Code: "08006"}
	})

	require.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestRunTransactionReplaysAfterRollback(t *testing.T) {
	t.Parallel()

	state := &transactionDriverState{}
	database := sql.OpenDB(transactionConnector{state: state})

	t.Cleanup(func() { require.NoError(t, database.Close()) })

	calls := 0
	err := postgresstore.RunTransactionForTest(t.Context(), database, "test transaction", func(*sql.Tx) error {
		calls++
		if calls == 1 {
			return &pgconn.PgError{Code: "40001"}
		}

		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	assert.Equal(t, int32(1), state.rollbacks.Load())
	assert.Equal(t, int32(1), state.commits.Load())
}

func TestRetryHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	calls := 0
	err := postgresstore.RunOperationForTest(ctx, "test operation", func() error {
		calls++

		return &pgconn.PgError{Code: "40001"}
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, calls)
}

func TestLeaseContextUsesEarlierBound(t *testing.T) {
	t.Parallel()

	parentDeadline := time.Now().Add(time.Hour)

	parent, cancelParent := context.WithDeadline(t.Context(), parentDeadline)
	defer cancelParent()

	leaseExpiry := time.Now().Add(time.Minute)

	ctx, cancel := postgresstore.LeaseContextForTest(parent, leaseExpiry)
	defer cancel()

	deadline, hasDeadline := ctx.Deadline()
	require.True(t, hasDeadline)
	assert.Equal(t, leaseExpiry, deadline)

	earlierParent, cancelEarlier := context.WithDeadline(t.Context(), time.Now().Add(time.Second))
	defer cancelEarlier()

	ctx, cancel = postgresstore.LeaseContextForTest(earlierParent, leaseExpiry)
	defer cancel()

	deadline, hasDeadline = ctx.Deadline()
	require.True(t, hasDeadline)
	parentDeadline, hasDeadline = earlierParent.Deadline()
	require.True(t, hasDeadline)
	assert.Equal(t, parentDeadline, deadline)
}

type transactionDriverState struct {
	commits   atomic.Int32
	rollbacks atomic.Int32
}

type transactionConnector struct{ state *transactionDriverState }

func (connector transactionConnector) Connect(context.Context) (driver.Conn, error) {
	return transactionConnection(connector), nil
}

func (transactionConnector) Driver() driver.Driver { return transactionDriver{} }

type transactionDriver struct{}

func (transactionDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("open is unsupported")
}

type transactionConnection struct{ state *transactionDriverState }

func (transactionConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is unsupported")
}

func (transactionConnection) Close() error { return nil }

func (connection transactionConnection) Begin() (driver.Tx, error) {
	return transaction(connection), nil
}

func (connection transactionConnection) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return transaction(connection), nil
}

type transaction struct{ state *transactionDriverState }

func (transaction transaction) Commit() error {
	transaction.state.commits.Add(1)

	return nil
}

func (transaction transaction) Rollback() error {
	transaction.state.rollbacks.Add(1)

	return nil
}
