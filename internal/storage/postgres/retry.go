package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/backoff"
)

const (
	transactionRetryAttempts = 3
	transactionRetryBase     = 10 * time.Millisecond
	transactionRetryMaximum  = 100 * time.Millisecond
)

// runTransaction retries a complete transaction after PostgreSQL errors that
// guarantee the failed transaction did not commit. Commit errors are never
// retried because the commit may have reached the server.
func runTransaction(
	ctx context.Context,
	database *sql.DB,
	operation string,
	transactionFunc func(*sql.Tx) error,
) error {
	return retry(ctx, operation, func() (bool, error) {
		return executeTransaction(ctx, database, transactionFunc)
	})
}

// runOperation retries a complete, atomic statement after PostgreSQL errors
// that guarantee the statement did not take effect.
func runOperation(ctx context.Context, operation string, operationFunc func() error) error {
	return retry(ctx, operation, func() (bool, error) {
		return true, operationFunc()
	})
}

func retry(
	ctx context.Context,
	operation string,
	operationFunc func() (safeToRetry bool, err error),
) error {
	for attempt := 1; attempt <= transactionRetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: %w", operation, err)
		}

		safeToRetry, err := operationFunc()
		if err == nil {
			return nil
		}

		if !safeToRetry || !isRetryable(err) || attempt == transactionRetryAttempts {
			return fmt.Errorf("%s: %w", operation, err)
		}

		delay := backoff.FullJitter(transactionRetryBase, transactionRetryMaximum, attempt)
		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return fmt.Errorf("%s: %w", operation, ctx.Err())
		case <-timer.C:
		}
	}

	return nil
}

func executeTransaction(
	ctx context.Context,
	database *sql.DB,
	transactionFunc func(*sql.Tx) error,
) (safeToRetry bool, err error) {
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return true, fmt.Errorf("begin transaction: %w", err)
	}

	if transactionErr := transactionFunc(transaction); transactionErr != nil {
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return false, errors.Join(transactionErr, rollbackErr)
		}

		return true, transactionErr
	}

	if commitErr := transaction.Commit(); commitErr != nil {
		return false, fmt.Errorf("commit transaction: %w", commitErr)
	}

	return false, nil
}

func isRetryable(err error) bool {
	var postgresErr interface{ SQLState() string }
	if !errors.As(err, &postgresErr) {
		return false
	}

	switch postgresErr.SQLState() {
	case "40001", "40P01":
		return true
	default:
		return false
	}
}

func leaseContext(ctx context.Context, leaseRemaining time.Duration) (context.Context, context.CancelFunc) {
	if leaseRemaining <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, leaseRemaining)
}
