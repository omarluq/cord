package postgres

import (
	"context"
	"database/sql"
	"time"
)

// RetryableForTest exposes retryable-error classification to external tests.
func RetryableForTest(err error) bool { return isRetryable(err) }

// RunOperationForTest exercises operation retry behavior in external tests.
func RunOperationForTest(ctx context.Context, operation string, operationFunc func() error) error {
	return runOperation(ctx, operation, operationFunc)
}

// RunTransactionForTest exercises transaction retry behavior in external tests.
func RunTransactionForTest(
	ctx context.Context,
	database *sql.DB,
	operation string,
	transactionFunc func(*sql.Tx) error,
) error {
	return runTransaction(ctx, database, operation, transactionFunc)
}

// LeaseContextForTest exposes lease-bounded context construction to external tests.
func LeaseContextForTest(ctx context.Context, expiry time.Time) (context.Context, context.CancelFunc) {
	return leaseContext(ctx, expiry)
}
