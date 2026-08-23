package sqlite

import (
	"context"
	"database/sql"
	"time"
)

// IsBusyForTest exposes busy-error classification to external driver tests.
func IsBusyForTest(err error) bool {
	return IsBusy(err)
}

// FencedTerminalTransitionForTest exercises transaction-attempt retry behavior.
func FencedTerminalTransitionForTest(
	ctx context.Context,
	store *Store,
	leaseRemaining time.Duration,
	transition func(context.Context, *sql.Tx) error,
) (bool, error) {
	return store.fencedTerminalTransition(
		ctx,
		leaseRemaining,
		func(attemptCtx context.Context, transaction *sql.Tx) error {
			return transition(attemptCtx, transaction)
		},
	)
}
