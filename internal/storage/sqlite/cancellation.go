package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

// CancelRun durably cancels a running or canceling run and all unfinished nodes.
// It returns false when the run is absent or already terminal.
func (s *Store) CancelRun(ctx context.Context, runID storage.RunID) (accepted bool, err error) {
	err = retryContention(ctx, "retry run cancellation", func(attemptCtx context.Context) error {
		accepted, err = s.cancelRunOnce(attemptCtx, runID)

		return err
	})

	return accepted, err
}

func (s *Store) cancelRunOnce(ctx context.Context, runID storage.RunID) (accepted bool, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin run cancellation: %w", err)
	}

	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback run cancellation: %w", rollbackErr))
		}
	}()

	result, err := transaction.ExecContext(ctx, `UPDATE cord_runs SET status = ?,
		updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE id = ? AND status IN (?, ?)`, storage.RunCanceling, runID, storage.RunRunning, storage.RunCanceling)
	if err != nil {
		return false, fmt.Errorf("request cancellation for run %q: %w", runID, err)
	}

	accepted, err = affectedOne(result)
	if err != nil {
		return false, fmt.Errorf("inspect run cancellation: %w", err)
	}

	if !accepted {
		return false, nil
	}

	cancelErr := cancelUnfinishedNodes(ctx, transaction, runID)
	if cancelErr != nil {
		return false, cancelErr
	}

	_, err = transaction.ExecContext(ctx, `UPDATE cord_runs SET status = ?,
		updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
		completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE id = ? AND status = ?`, storage.RunCanceled, runID, storage.RunCanceling)
	if err != nil {
		return false, fmt.Errorf("finish cancellation for run %q: %w", runID, err)
	}

	if err = transaction.Commit(); err != nil {
		return false, fmt.Errorf("commit run cancellation: %w", err)
	}

	return true, nil
}

func cancelUnfinishedNodes(ctx context.Context, transaction *sql.Tx, runID storage.RunID) error {
	_, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
		SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
			completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE run_id = ? AND status IN (?, ?, ?, ?)`,
		storage.NodeCanceled, runID, storage.NodePending, storage.NodeReady, storage.NodeRunning, storage.NodeRetryWait)
	if err != nil {
		return fmt.Errorf("cancel unfinished nodes for run %q: %w", runID, err)
	}

	return nil
}
