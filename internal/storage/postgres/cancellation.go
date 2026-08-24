package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// CancelRun atomically terminalizes a running run, cancels unfinished nodes,
// and reports the durable state that decided the request.
func (s *Store) CancelRun(
	ctx context.Context,
	runID storage.RunID,
) (outcome storage.CancellationOutcome, err error) {
	err = runTransaction(ctx, s.pool, "cancel run", func(transaction *sql.Tx) error {
		outcome = ""

		transitionedAt, timeErr := databaseInstant(ctx, transaction)
		if timeErr != nil {
			return timeErr
		}

		status, lockErr := lockRunForCancellation(ctx, transaction, runID)
		if lockErr != nil {
			return lockErr
		}

		terminalOutcome, terminal, outcomeErr := cancellationOutcome(runID, status)
		if outcomeErr != nil {
			return outcomeErr
		}

		if terminal {
			outcome = terminalOutcome

			return nil
		}

		if requestErr := requestRunCancellation(
			ctx, transaction, runID, transitionedAt,
		); requestErr != nil {
			return requestErr
		}

		if cancelErr := cancelUnfinishedNodes(
			ctx, transaction, runID, storage.ReasonCanceledByRequest, transitionedAt,
		); cancelErr != nil {
			return cancelErr
		}

		if finishErr := finishRunCancellation(
			ctx, transaction, runID, transitionedAt,
		); finishErr != nil {
			return finishErr
		}

		outcome = storage.CancellationCanceled

		return nil
	})
	if err != nil {
		return "", err
	}

	return outcome, nil
}

func cancellationOutcome(
	runID storage.RunID,
	status storage.RunStatus,
) (storage.CancellationOutcome, bool, error) {
	switch status {
	case "":
		return storage.CancellationNotFound, true, nil
	case storage.RunCanceled:
		return storage.CancellationAlreadyCanceled, true, nil
	case storage.RunCompleted, storage.RunFailed:
		return storage.CancellationFinished, true, nil
	case storage.RunRunning, storage.RunCanceling:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("cancel run %q: unknown run status %q", runID, status)
	}
}

func lockRunForCancellation(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
) (storage.RunStatus, error) {
	const query = `SELECT status FROM cord_runs WHERE id = $1 FOR UPDATE`

	var status storage.RunStatus
	if err := transaction.QueryRowContext(ctx, query, runID).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}

		return "", fmt.Errorf("lock run %q for cancellation: %w", runID, err)
	}

	return status, nil
}

func requestRunCancellation(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	transitionedAt time.Time,
) error {
	const query = `UPDATE cord_runs
		SET status = $1, updated_at = $5
		WHERE id = $2 AND status IN ($3, $4)`

	result, err := transaction.ExecContext(
		ctx, query, storage.RunCanceling, runID, storage.RunRunning, storage.RunCanceling,
		transitionedAt,
	)
	if err != nil {
		return fmt.Errorf("request cancellation for run %q: %w", runID, err)
	}

	return requireOneAffected(result, "run cancellation request")
}

func finishRunCancellation(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	transitionedAt time.Time,
) error {
	const query = `UPDATE cord_runs
		SET status = $1,
			updated_at = $4,
			completed_at = $4,
			terminal_reason = CASE
				WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN $5 ELSE terminal_reason
			END,
			terminal_runner_id = CASE
				WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN NULL ELSE terminal_runner_id
			END,
		lifecycle_version = COALESCE(lifecycle_version, 1)
		WHERE id = $2 AND status = $3`

	result, err := transaction.ExecContext(
		ctx, query, storage.RunCanceled, runID, storage.RunCanceling,
		transitionedAt, storage.ReasonCanceledByRequest,
	)
	if err != nil {
		return fmt.Errorf("finish cancellation for run %q: %w", runID, err)
	}

	if err = requireOneAffected(result, "finished run cancellation"); err != nil {
		return fmt.Errorf("finish cancellation for run %q: %w", runID, err)
	}

	return nil
}
