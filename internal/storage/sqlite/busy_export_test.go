package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// IsBusyForTest exposes busy-error classification to external driver tests.
func IsBusyForTest(err error) bool {
	return IsBusy(err)
}

// CancelRunForTest exercises quarantined cancellation groundwork without
// exporting it from the concrete store or backend contract.
func CancelRunForTest(ctx context.Context, store *Store, runID storage.RunID) (bool, error) {
	return store.cancelRun(ctx, runID)
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
