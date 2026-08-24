package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// RetryNode records one transient failure and releases its lease.
func (s *Store) RetryNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	failure storage.EncodedPayload,
	delay time.Duration,
) (bool, error) {
	if delay < 0 {
		return false, errors.New("schedule node retry: delay must not be negative")
	}

	modifier := durationModifier(delay)

	var accepted bool

	err := retryFencedContention(
		ctx, "retry scheduling node retry", lease.Remaining,
		func(attemptCtx context.Context) (operationErr error) {
			transaction, beginErr := s.database.BeginTx(attemptCtx, nil)
			if beginErr != nil {
				return fmt.Errorf("begin node retry: %w", beginErr)
			}
			defer func() {
				operationErr = joinRollbackError(transaction.Rollback(), "rollback node retry", operationErr)
			}()

			transitionedAt, timeErr := databaseInstant(attemptCtx, transaction)
			if timeErr != nil {
				return timeErr
			}

			instant := formatTime(transitionedAt)

			result, execErr := transaction.ExecContext(attemptCtx, `UPDATE cord_nodes SET status = ?, error_payload = ?,
			available_at = strftime('%Y-%m-%dT%H:%M:%fZ', ?, ?), lease_owner = NULL, lease_expires_at = NULL,
			state_changed_at = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE state_changed_at END,
			lifecycle_version = COALESCE(lifecycle_version, 1)
			WHERE run_id = ? AND node_id = ? AND status = ? AND lease_owner = ? AND lease_generation = ?
			AND julianday(lease_expires_at) > julianday('now')
			AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)`,
				storage.NodeRetryWait, nullPayload(failure), instant, modifier, instant,
				runID, nodeID, storage.NodeRunning, lease.Owner, lease.Generation,
				runID, storage.RunRunning)
			if execErr != nil {
				return fmt.Errorf("schedule node retry: %w", execErr)
			}

			accepted, execErr = affectedOne(result)
			if execErr != nil || !accepted {
				return execErr
			}

			if execErr = transaction.Commit(); execErr != nil {
				return fmt.Errorf("commit node retry: %w", execErr)
			}

			return nil
		},
	)

	return accepted, err
}

// PromoteRetries makes retry deadlines eligible according to database time.
func (s *Store) PromoteRetries(ctx context.Context) (count int64, err error) {
	err = retryContention(ctx, "retry promote retries", func(attemptCtx context.Context) (operationErr error) {
		transaction, beginErr := s.database.BeginTx(attemptCtx, nil)
		if beginErr != nil {
			return fmt.Errorf("begin retry promotion: %w", beginErr)
		}
		defer func() {
			operationErr = joinRollbackError(transaction.Rollback(), "rollback retry promotion", operationErr)
		}()

		transitionedAt, timeErr := databaseInstant(attemptCtx, transaction)
		if timeErr != nil {
			return timeErr
		}

		instant := formatTime(transitionedAt)

		result, execErr := transaction.ExecContext(attemptCtx, `UPDATE cord_nodes
			SET status = ?, state_changed_at = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE state_changed_at END,
			lifecycle_version = COALESCE(lifecycle_version, 1)
			WHERE status = ? AND julianday(available_at) <= julianday(?)
			AND EXISTS (SELECT 1 FROM cord_runs WHERE id = run_id AND status = ?)`,
			storage.NodeReady, instant, storage.NodeRetryWait, instant, storage.RunRunning)
		if execErr != nil {
			return fmt.Errorf("promote retries: %w", execErr)
		}

		count, execErr = result.RowsAffected()
		if execErr != nil {
			return fmt.Errorf("inspect promote retries: %w", execErr)
		}

		if execErr = transaction.Commit(); execErr != nil {
			return fmt.Errorf("commit retry promotion: %w", execErr)
		}

		return nil
	})

	return count, err
}

// FailNode accepts a permanent failure only from the current, unexpired lease.
// It fails the run and cancels all other unfinished nodes atomically.
func (s *Store) FailNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	failure storage.EncodedPayload,
	reason storage.TerminalReason,
) (bool, error) {
	return s.fencedTerminalTransition(
		ctx, lease.Remaining, func(attemptCtx context.Context, transaction *sql.Tx) error {
			transitionedAt, err := databaseInstant(attemptCtx, transaction)
			if err != nil {
				return err
			}

			instant := formatTime(transitionedAt)

			result, err := transaction.ExecContext(attemptCtx, `UPDATE cord_nodes
			SET status = ?, error_payload = ?, lease_owner = NULL, lease_expires_at = NULL,
				completed_at = ?,
				state_changed_at = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE state_changed_at END,
				terminal_reason = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE terminal_reason END,
			lifecycle_version = COALESCE(lifecycle_version, 1)
			WHERE run_id = ? AND node_id = ? AND status = ?
				AND lease_owner = ? AND lease_generation = ?
				AND julianday(lease_expires_at) > julianday('now')
				AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)`,
				storage.NodeFailed, nullPayload(failure), instant, instant,
				reason, runID, nodeID, storage.NodeRunning,
				lease.Owner, lease.Generation, runID, storage.RunRunning)
			if err != nil {
				return fmt.Errorf("fail node %q for run %q: %w", nodeID, runID, err)
			}

			accepted, err := affectedOne(result)
			if err != nil {
				return fmt.Errorf("inspect permanent failure: %w", err)
			}

			if !accepted {
				return errFenceRejected
			}

			cancelErr := cancelUnfinishedNodes(
				attemptCtx, transaction, runID, storage.ReasonCanceledByRunFailure, instant,
			)
			if cancelErr != nil {
				return cancelErr
			}

			_, err = transaction.ExecContext(attemptCtx, `UPDATE cord_runs
			SET status = ?, error_payload = ?, updated_at = ?, completed_at = ?,
				terminal_reason = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE terminal_reason END,
				terminal_runner_id = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE terminal_runner_id END,
			lifecycle_version = COALESCE(lifecycle_version, 1)
			WHERE id = ? AND status = ?`, storage.RunFailed, nullPayload(failure), instant, instant,
				reason, lease.Owner, runID, storage.RunRunning)
			if err != nil {
				return fmt.Errorf("fail run %q: %w", runID, err)
			}

			return nil
		},
	)
}
