package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// RetryableForTest exposes retryable-error classification to external tests.
func RetryableForTest(err error) bool { return isRetryable(err) }

// CancelRunForTest exercises quarantined cancellation groundwork without
// exporting it from the concrete store or backend contract.
func CancelRunForTest(ctx context.Context, store *Store, runID storage.RunID) (bool, error) {
	return store.cancelRun(ctx, runID)
}

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
func LeaseContextForTest(ctx context.Context, remaining time.Duration) (context.Context, context.CancelFunc) {
	return leaseContext(ctx, remaining)
}
