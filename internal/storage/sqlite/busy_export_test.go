package sqlite

import (
	"context"
	"database/sql"
	"time"
)

// IsBusyForTest exposes busy-error classification to external driver tests.
func IsBusyForTest(err error) bool {
	return isBusy(err)
}

// FencedTerminalTransitionForTest exercises transaction-attempt retry behavior.
func FencedTerminalTransitionForTest(
	ctx context.Context,
	store *Store,
	leaseExpiresAt time.Time,
	transition func(*sql.Tx) error,
) (bool, error) {
	return store.fencedTerminalTransition(ctx, leaseExpiresAt, func(_ context.Context, transaction *sql.Tx) error {
		return transition(transaction)
	})
}
