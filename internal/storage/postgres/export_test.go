package postgres

import (
	"context"
	"database/sql"
	"time"
)

func RetryableForTest(err error) bool { return isRetryable(err) }

func RunOperationForTest(ctx context.Context, operation string, operationFunc func() error) error {
	return runOperation(ctx, operation, operationFunc)
}

func RunTransactionForTest(
	ctx context.Context,
	database *sql.DB,
	operation string,
	transactionFunc func(*sql.Tx) error,
) error {
	return runTransaction(ctx, database, operation, transactionFunc)
}

func LeaseContextForTest(ctx context.Context, expiry time.Time) (context.Context, context.CancelFunc) {
	return leaseContext(ctx, expiry)
}
