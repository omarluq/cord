package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

// CancelRun durably cancels unfinished work and reports the persisted state
// that decided the request.
func (s *Store) CancelRun(
	ctx context.Context,
	runID storage.RunID,
) (outcome storage.CancellationOutcome, err error) {
	err = retryContention(ctx, "retry run cancellation", func(attemptCtx context.Context) error {
		outcome, err = s.cancelRunOnce(attemptCtx, runID)

		return err
	})

	return outcome, err
}

func (s *Store) cancelRunOnce(
	ctx context.Context,
	runID storage.RunID,
) (outcome storage.CancellationOutcome, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin run cancellation: %w", err)
	}

	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback run cancellation: %w", rollbackErr))
		}
	}()

	transitionedAt, err := databaseInstant(ctx, transaction)
	if err != nil {
		return "", err
	}

	instant := formatTime(transitionedAt)

	result, err := transaction.ExecContext(ctx, `UPDATE cord_runs SET status = ?, updated_at = ?
		WHERE id = ? AND status IN (?, ?)`, storage.RunCanceling, instant,
		runID, storage.RunRunning, storage.RunCanceling)
	if err != nil {
		return "", fmt.Errorf("request cancellation for run %q: %w", runID, err)
	}

	accepted, err := affectedOne(result)
	if err != nil {
		return "", fmt.Errorf("inspect run cancellation: %w", err)
	}

	outcome, err = persistCancellation(ctx, transaction, runID, accepted, instant)
	if err != nil {
		return "", err
	}

	if err = transaction.Commit(); err != nil {
		return "", fmt.Errorf("commit run cancellation: %w", err)
	}

	return outcome, nil
}

func persistCancellation(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	accepted bool,
	instant string,
) (storage.CancellationOutcome, error) {
	if !accepted {
		return cancellationOutcome(ctx, transaction, runID)
	}

	if err := cancelUnfinishedNodes(
		ctx, transaction, runID, storage.ReasonCanceledByRequest, instant,
	); err != nil {
		return "", err
	}

	if err := finishRunCancellation(ctx, transaction, runID, instant); err != nil {
		return "", err
	}

	return storage.CancellationCanceled, nil
}

func cancellationOutcome(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
) (storage.CancellationOutcome, error) {
	var status storage.RunStatus

	row := transaction.QueryRowContext(ctx, "SELECT status FROM cord_runs WHERE id = ?", runID)
	if err := row.Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.CancellationNotFound, nil
		}

		return "", fmt.Errorf("inspect run %q after rejected cancellation: %w", runID, err)
	}

	switch status {
	case storage.RunCanceled:
		return storage.CancellationAlreadyCanceled, nil
	case storage.RunCompleted, storage.RunFailed:
		return storage.CancellationFinished, nil
	case storage.RunRunning, storage.RunCanceling:
		return "", fmt.Errorf("cancel run %q: unexpected nonterminal persisted status %q", runID, status)
	default:
		return "", fmt.Errorf("cancel run %q: unexpected persisted status %q", runID, status)
	}
}

func finishRunCancellation(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	instant string,
) error {
	result, err := transaction.ExecContext(ctx, `UPDATE cord_runs SET status = ?,
		updated_at = ?, completed_at = ?,
		terminal_reason = ?,
		terminal_runner_id = NULL
				WHERE id = ? AND status = ?`, storage.RunCanceled, instant, instant,
		storage.ReasonCanceledByRequest, runID, storage.RunCanceling)
	if err != nil {
		return fmt.Errorf("finish cancellation for run %q: %w", runID, err)
	}

	finished, err := affectedOne(result)
	if err != nil {
		return fmt.Errorf("inspect finished run cancellation: %w", err)
	}

	if !finished {
		return fmt.Errorf("finish cancellation for run %q: run is no longer canceling", runID)
	}

	return nil
}

func cancelUnfinishedNodes(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	reason storage.TerminalReason,
	instant string,
) error {
	_, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
		SET status = ?, lease_owner = NULL, lease_expires_at = NULL, completed_at = ?,
			state_changed_at = ?,
			terminal_reason = ?
				WHERE run_id = ? AND status IN (?, ?, ?, ?)`,
		storage.NodeCanceled, instant, instant, reason, runID,
		storage.NodePending, storage.NodeReady, storage.NodeRunning, storage.NodeRetryWait)
	if err != nil {
		return fmt.Errorf("cancel unfinished nodes for run %q: %w", runID, err)
	}

	return nil
}
