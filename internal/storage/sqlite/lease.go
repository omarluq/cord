package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const exhaustedLeaseRecoveryBatchSize = 100

// RecoverExpiredLeases recovers abandoned nodes with a newer fence. Nodes with
// another attempt become ready; nodes whose final attempt expired atomically
// fail their run and cancel unfinished siblings.
func (s *Store) RecoverExpiredLeases(ctx context.Context) (count int64, err error) {
	err = retryContention(ctx, "retry expired lease recovery", func(attemptCtx context.Context) error {
		count, err = s.recoverExpiredLeasesOnce(attemptCtx)

		return err
	})

	return count, err
}

type expiredNode struct {
	runID       storage.RunID
	nodeID      storage.NodeID
	functionKey string
	attempt     int
}

func (s *Store) recoverExpiredLeasesOnce(ctx context.Context) (count int64, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin expired lease recovery: %w", err)
	}
	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback expired lease recovery: %w", rollbackErr))
		}
	}()

	transitionedAt, err := databaseInstant(ctx, transaction)
	if err != nil {
		return 0, err
	}

	instant := formatTime(transitionedAt)

	count, err = recoverExpiredLeasesInTransaction(ctx, transaction, transitionedAt, instant)
	if err != nil {
		return 0, err
	}

	if err = transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit expired lease recovery: %w", err)
	}

	return count, nil
}

func recoverExpiredLeasesInTransaction(
	ctx context.Context,
	transaction *sql.Tx,
	transitionedAt time.Time,
	instant string,
) (int64, error) {
	count, err := recoverRetryableExpiredLeases(ctx, transaction, instant)
	if err != nil {
		return 0, err
	}

	exhausted, err := findExhaustedExpiredLeases(ctx, transaction, instant)
	if err != nil {
		return 0, err
	}

	for _, node := range exhausted {
		recovered, recoverErr := recoverExhaustedLease(ctx, transaction, node, transitionedAt, instant)
		if recoverErr != nil {
			return 0, recoverErr
		}

		if recovered {
			count++
		}
	}

	return count, nil
}

func recoverRetryableExpiredLeases(
	ctx context.Context,
	transaction *sql.Tx,
	instant string,
) (int64, error) {
	result, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
		SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
			lease_generation = lease_generation + 1,
			state_changed_at = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE state_changed_at END,
		lifecycle_version = COALESCE(lifecycle_version, 1)
		WHERE status = ? AND julianday(lease_expires_at) <= julianday(?)
			AND EXISTS (SELECT 1 FROM cord_runs
				WHERE id = run_id AND status = ? AND cord_nodes.attempt < max_attempts)`,
		storage.NodeReady, instant, storage.NodeRunning, instant, storage.RunRunning)
	if err != nil {
		return 0, fmt.Errorf("recover retryable expired leases: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect retryable expired lease recovery: %w", err)
	}

	return count, nil
}

func findExhaustedExpiredLeases(
	ctx context.Context,
	transaction *sql.Tx,
	instant string,
) (nodes []expiredNode, err error) {
	rows, err := transaction.QueryContext(ctx, `SELECT n.run_id, n.node_id, n.function_key, n.attempt
		FROM cord_nodes AS n JOIN cord_runs AS r ON r.id = n.run_id
		WHERE n.status = ? AND julianday(n.lease_expires_at) <= julianday(?)
			AND r.status = ? AND n.attempt >= r.max_attempts
		ORDER BY n.run_id, n.node_id
		LIMIT ?`, storage.NodeRunning, instant, storage.RunRunning, exhaustedLeaseRecoveryBatchSize)
	if err != nil {
		return nil, fmt.Errorf("find exhausted expired leases: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close exhausted expired leases: %w", closeErr))
		}
	}()

	nodes = make([]expiredNode, 0)

	for rows.Next() {
		var node expiredNode
		if scanErr := rows.Scan(&node.runID, &node.nodeID, &node.functionKey, &node.attempt); scanErr != nil {
			return nil, fmt.Errorf("scan exhausted expired lease: %w", scanErr)
		}

		nodes = append(nodes, node)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate exhausted expired leases: %w", err)
	}

	return nodes, nil
}

func recoverExhaustedLease(
	ctx context.Context,
	transaction *sql.Tx,
	node expiredNode,
	transitionedAt time.Time,
	instant string,
) (bool, error) {
	failure := storage.EncodeLeaseExpiryFailure(node.nodeID, node.functionKey, node.attempt, transitionedAt)

	result, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
		SET status = ?, error_payload = ?, lease_owner = NULL, lease_expires_at = NULL,
			lease_generation = lease_generation + 1, completed_at = ?,
			state_changed_at = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE state_changed_at END,
			terminal_reason = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE terminal_reason END,
		lifecycle_version = COALESCE(lifecycle_version, 1)
		WHERE run_id = ? AND node_id = ? AND status = ?
			AND julianday(lease_expires_at) <= julianday(?)
			AND EXISTS (SELECT 1 FROM cord_runs
				WHERE id = run_id AND status = ? AND cord_nodes.attempt >= max_attempts)`,
		storage.NodeFailed, nullPayload(failure), instant, instant, storage.ReasonFailureLeaseExpired,
		node.runID, node.nodeID, storage.NodeRunning, instant, storage.RunRunning)
	if err != nil {
		return false, fmt.Errorf("fail exhausted node %q for run %q: %w", node.nodeID, node.runID, err)
	}

	accepted, err := affectedOne(result)
	if err != nil {
		return false, fmt.Errorf("inspect exhausted node recovery: %w", err)
	}

	if !accepted {
		return false, nil
	}

	if cancelErr := cancelUnfinishedNodes(
		ctx, transaction, node.runID, storage.ReasonCanceledByRunFailure, instant,
	); cancelErr != nil {
		return false, cancelErr
	}

	runResult, err := transaction.ExecContext(ctx, `UPDATE cord_runs
		SET status = ?, output_payload = NULL, error_payload = ?, updated_at = ?, completed_at = ?,
			terminal_reason = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN ? ELSE terminal_reason END,
			terminal_runner_id = CASE
			WHEN lifecycle_version IS NULL OR lifecycle_version = 1 THEN NULL ELSE terminal_runner_id END,
		lifecycle_version = COALESCE(lifecycle_version, 1)
		WHERE id = ? AND status = ?`,
		storage.RunFailed, nullPayload(failure), instant, instant, storage.ReasonFailureLeaseExpired,
		node.runID, storage.RunRunning)
	if err != nil {
		return false, fmt.Errorf("fail run %q after exhausted lease: %w", node.runID, err)
	}

	runAccepted, err := affectedOne(runResult)
	if err != nil {
		return false, fmt.Errorf("inspect exhausted run recovery: %w", err)
	}

	if !runAccepted {
		return false, fmt.Errorf("fail run %q after exhausted lease: running run disappeared", node.runID)
	}

	return true, nil
}

// HeartbeatNode extends an exact active lease using database time and returns
// its database-relative remaining lifetime.
func (s *Store) HeartbeatNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	ttl time.Duration,
) (bool, time.Duration, error) {
	if ttl <= 0 {
		return false, 0, errors.New("heartbeat node lease: TTL must be positive")
	}

	modifier := durationModifier(ttl)

	var remainingMicros int64

	accepted := false

	err := retryFencedContention(ctx, "retry node heartbeat", lease.Remaining, func(attemptCtx context.Context) error {
		scanErr := s.database.QueryRowContext(attemptCtx, `UPDATE cord_nodes
			SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
			WHERE run_id = ? AND node_id = ? AND status = ? AND lease_owner = ? AND lease_generation = ?
			AND julianday(lease_expires_at) > julianday('now')
			AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)
			RETURNING MAX(0, CAST((julianday(lease_expires_at) - julianday('now')) * 86400000000 AS INTEGER))`,
			modifier, runID, nodeID, storage.NodeRunning, lease.Owner, lease.Generation,
			runID, storage.RunRunning).Scan(&remainingMicros)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}

		if scanErr != nil {
			return fmt.Errorf("heartbeat node lease: %w", scanErr)
		}

		accepted = true

		return nil
	})
	if err != nil {
		return false, 0, err
	}

	if !accepted {
		return false, 0, nil
	}

	return true, time.Duration(remainingMicros) * time.Microsecond, nil
}

func durationModifier(duration time.Duration) string {
	seconds := strconv.FormatFloat(duration.Seconds(), 'f', 6, 64)
	if duration >= 0 {
		seconds = "+" + seconds
	}

	return seconds + " seconds"
}
