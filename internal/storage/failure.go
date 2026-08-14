package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RetryNode records one transient failure and releases its lease.
func (s *Store) RetryNode(
	ctx context.Context,
	runID RunID,
	nodeID NodeID,
	lease Lease,
	failure EncodedPayload,
	delay time.Duration,
) (bool, error) {
	if delay < 0 {
		return false, errors.New("schedule node retry: delay must not be negative")
	}

	modifier := sqliteDurationModifier(delay)

	result, err := s.database.ExecContext(ctx, `UPDATE cord_nodes SET status = ?, error_payload = ?,
		available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?), lease_owner = NULL, lease_expires_at = NULL
		WHERE run_id = ? AND node_id = ? AND status = ? AND lease_owner = ? AND lease_generation = ?
		AND julianday(lease_expires_at) > julianday('now')
		AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)`,
		NodeRetryWait, nullPayload(failure), modifier, runID, nodeID, NodeRunning,
		lease.Owner, lease.Generation, runID, RunRunning)
	if err != nil {
		return false, fmt.Errorf("schedule node retry: %w", err)
	}

	return affectedOne(result)
}

// PromoteRetries makes retry deadlines eligible according to database time.
func (s *Store) PromoteRetries(ctx context.Context) (int64, error) {
	query := `UPDATE cord_nodes SET status = ? WHERE status = ?
		AND julianday(available_at) <= julianday('now')
		AND EXISTS (SELECT 1 FROM cord_runs WHERE id = run_id AND status = ?)`

	return s.updateNodes(ctx, query, "promote retries", NodeReady, NodeRetryWait, RunRunning)
}

// FailNode accepts a permanent failure only from the current, unexpired lease.
// It fails the run and cancels all other unfinished nodes atomically.
func (s *Store) FailNode(
	ctx context.Context,
	runID RunID,
	nodeID NodeID,
	lease Lease,
	failure EncodedPayload,
) (bool, error) {
	return s.fencedTerminalTransition(ctx, func(transaction *sql.Tx) error {
		result, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
			SET status = ?, error_payload = ?, lease_owner = NULL, lease_expires_at = NULL,
				completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
			WHERE run_id = ? AND node_id = ? AND status = ?
				AND lease_owner = ? AND lease_generation = ?
				AND julianday(lease_expires_at) > julianday('now')
				AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)`,
			NodeFailed, nullPayload(failure), runID, nodeID, NodeRunning,
			lease.Owner, lease.Generation, runID, RunRunning)
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

		cancelErr := cancelUnfinishedNodes(ctx, transaction, runID)
		if cancelErr != nil {
			return cancelErr
		}

		_, err = transaction.ExecContext(ctx, `UPDATE cord_runs
			SET status = ?, error_payload = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
				completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
			WHERE id = ? AND status = ?`, RunFailed, nullPayload(failure), runID, RunRunning)
		if err != nil {
			return fmt.Errorf("fail run %q: %w", runID, err)
		}

		return nil
	})
}
