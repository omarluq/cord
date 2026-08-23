package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

// CancelRun durably cancels a running or canceling run and all unfinished nodes.
// It returns false when the run is absent or already terminal.
func (s *Store) CancelRun(ctx context.Context, runID storage.RunID) (bool, error) {
	accepted := false

	err := runTransaction(ctx, s.database, "cancel run", func(transaction *sql.Tx) error {
		accepted = false

		requested, requestErr := requestRunCancellation(ctx, transaction, runID)
		if requestErr != nil || !requested {
			return requestErr
		}

		if cancelErr := cancelUnfinishedNodes(ctx, transaction, runID); cancelErr != nil {
			return cancelErr
		}

		if finishErr := finishRunCancellation(ctx, transaction, runID); finishErr != nil {
			return finishErr
		}

		accepted = true

		return nil
	})
	if err != nil {
		return false, err
	}

	return accepted, nil
}

func requestRunCancellation(ctx context.Context, transaction *sql.Tx, runID storage.RunID) (bool, error) {
	const query = `UPDATE cord_runs
		SET status = $1, updated_at = clock_timestamp()
		WHERE id = $2 AND status IN ($3, $4)`

	result, err := transaction.ExecContext(
		ctx, query, storage.RunCanceling, runID, storage.RunRunning, storage.RunCanceling,
	)
	if err != nil {
		return false, fmt.Errorf("request cancellation for run %q: %w", runID, err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect run cancellation: %w", err)
	}

	return count == 1, nil
}

func finishRunCancellation(ctx context.Context, transaction *sql.Tx, runID storage.RunID) error {
	const query = `UPDATE cord_runs
		SET status = $1,
			updated_at = clock_timestamp(),
			completed_at = clock_timestamp()
		WHERE id = $2 AND status = $3`

	if _, err := transaction.ExecContext(
		ctx, query, storage.RunCanceled, runID, storage.RunCanceling,
	); err != nil {
		return fmt.Errorf("finish cancellation for run %q: %w", runID, err)
	}

	return nil
}
