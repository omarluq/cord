package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// CompleteNode records a successful result under an active lease.
func (s *Store) CompleteNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	payload storage.EncodedPayload,
) (bool, error) {
	return s.terminal(ctx, runID, nodeID, lease, payload, true, storage.ReasonSucceeded)
}

// FailNode records a terminal failure under an active lease.
func (s *Store) FailNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	payload storage.EncodedPayload,
	reason storage.TerminalReason,
) (bool, error) {
	return s.terminal(ctx, runID, nodeID, lease, payload, false, reason)
}

// RetryNode records a transient failure and schedules another attempt.
func (s *Store) RetryNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	payload storage.EncodedPayload,
	delay time.Duration,
) (bool, error) {
	if delay < 0 {
		return false, errors.New("schedule node retry: delay must not be negative")
	}

	const query = `UPDATE cord_nodes
		SET status = 'retry_wait',
			error_payload = $1,
			available_at = $7::timestamptz + ($2 * interval '1 microsecond'),
			lease_owner = NULL,
			lease_expires_at = NULL,
			state_changed_at = $7
				WHERE run_id = $3
			AND node_id = $4
			AND status = 'running'
			AND lease_owner = $5
			AND lease_generation = $6
			AND lease_expires_at > $7::timestamptz
			AND EXISTS (
				SELECT 1 FROM cord_runs WHERE id = $3 AND status = 'running'
			)`

	retryCtx, cancel := leaseContext(ctx, lease.Remaining)
	defer cancel()

	accepted := false
	err := runTransaction(retryCtx, s.pool, "schedule node retry", func(transaction *sql.Tx) error {
		accepted = false

		transitionedAt, timeErr := databaseInstant(retryCtx, transaction)
		if timeErr != nil {
			return timeErr
		}

		result, execErr := transaction.ExecContext(
			retryCtx,
			query,
			nullablePayload(payload),
			delay.Microseconds(),
			runID,
			nodeID,
			lease.Owner,
			lease.Generation,
			transitionedAt,
		)
		if execErr != nil {
			return fmt.Errorf("execute node retry: %w", execErr)
		}

		count, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("inspect scheduled node retry: %w", rowsErr)
		}

		accepted = count == 1

		return nil
	})

	return accepted, err
}

// PromoteRetries makes elapsed retries ready.
func (s *Store) PromoteRetries(ctx context.Context) (count int64, err error) {
	err = runTransaction(ctx, s.pool, "promote retries", func(transaction *sql.Tx) error {
		transitionedAt, timeErr := databaseInstant(ctx, transaction)
		if timeErr != nil {
			return timeErr
		}

		result, execErr := transaction.ExecContext(ctx, `UPDATE cord_nodes
			SET status = 'ready',
				state_changed_at = $1
						WHERE status = 'retry_wait' AND available_at <= $1
				AND EXISTS (SELECT 1 FROM cord_runs
					WHERE id = run_id AND status = 'running')`, transitionedAt)
		if execErr != nil {
			return fmt.Errorf("execute promote retries: %w", execErr)
		}

		count, execErr = result.RowsAffected()
		if execErr != nil {
			return fmt.Errorf("inspect promote retries: %w", execErr)
		}

		return nil
	})

	return count, err
}
