package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

type expiredNode struct {
	runID       storage.RunID
	nodeID      storage.NodeID
	functionKey string
	attempt     int
	maxAttempts int
}

// RecoverExpiredLeases recovers abandoned nodes with a newer fence. Nodes with
// another attempt become ready; nodes whose final attempt expired atomically
// fail their run and cancel unfinished siblings.
func (s *Store) RecoverExpiredLeases(ctx context.Context) (count int64, err error) {
	err = runTransaction(ctx, s.pool, "recover expired leases", func(transaction *sql.Tx) error {
		count = 0

		transitionedAt, timeErr := databaseInstant(ctx, transaction)
		if timeErr != nil {
			return timeErr
		}

		expired, loadErr := loadExpiredNodes(ctx, transaction, transitionedAt)
		if loadErr != nil {
			return loadErr
		}

		for _, node := range expired {
			affected, recoverErr := recoverExpiredNode(ctx, transaction, node, transitionedAt)
			if recoverErr != nil {
				return recoverErr
			}

			count += affected
		}

		return nil
	})

	return count, err
}

func loadExpiredNodes(
	ctx context.Context,
	transaction *sql.Tx,
	transitionedAt time.Time,
) (_ []expiredNode, err error) {
	rows, err := transaction.QueryContext(ctx, `SELECT n.run_id, n.node_id,
		n.function_key, n.attempt, r.max_attempts
		FROM cord_nodes AS n JOIN cord_runs AS r ON r.id = n.run_id
		WHERE n.status = 'running' AND n.lease_expires_at <= $1
			AND r.status = 'running'
		ORDER BY n.run_id, n.node_id
		FOR UPDATE OF r, n SKIP LOCKED`, transitionedAt)
	if err != nil {
		return nil, fmt.Errorf("find expired leases: %w", err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()

	expired := make([]expiredNode, 0)

	for rows.Next() {
		var node expiredNode
		if scanErr := rows.Scan(
			&node.runID, &node.nodeID, &node.functionKey, &node.attempt, &node.maxAttempts,
		); scanErr != nil {
			return nil, fmt.Errorf("scan expired lease: %w", scanErr)
		}

		expired = append(expired, node)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("iterate expired leases: %w", rowsErr)
	}

	return expired, nil
}

func recoverExpiredNode(
	ctx context.Context,
	transaction *sql.Tx,
	node expiredNode,
	transitionedAt time.Time,
) (int64, error) {
	if node.attempt < node.maxAttempts {
		return recoverRetryableExpiredNode(ctx, transaction, node, transitionedAt)
	}

	return recoverExhaustedNode(ctx, transaction, node, transitionedAt)
}

func recoverRetryableExpiredNode(
	ctx context.Context,
	transaction *sql.Tx,
	node expiredNode,
	transitionedAt time.Time,
) (int64, error) {
	result, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
		SET status = 'ready', lease_owner = NULL, lease_expires_at = NULL,
			lease_generation = lease_generation + 1,
			state_changed_at = $3
				WHERE run_id = $1 AND node_id = $2 AND status = 'running'
			AND lease_expires_at <= $3
			AND EXISTS (SELECT 1 FROM cord_runs
				WHERE id = $1 AND status = 'running')`, node.runID, node.nodeID, transitionedAt)
	if err != nil {
		return 0, fmt.Errorf("recover retryable expired lease: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect retryable expired lease recovery: %w", err)
	}

	return affected, nil
}

func recoverExhaustedNode(
	ctx context.Context,
	transaction *sql.Tx,
	node expiredNode,
	transitionedAt time.Time,
) (int64, error) {
	failure := storage.EncodeLeaseExpiryFailure(
		node.nodeID, node.functionKey, node.attempt, transitionedAt,
	)

	result, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
		SET status = 'failed', error_payload = $1, lease_owner = NULL,
			lease_expires_at = NULL, lease_generation = lease_generation + 1,
			completed_at = $4,
			state_changed_at = $4,
			terminal_reason = $5
				WHERE run_id = $2 AND node_id = $3 AND status = 'running'
			AND lease_expires_at <= $4
			AND EXISTS (SELECT 1 FROM cord_runs
				WHERE id = $2 AND status = 'running')`,
		nullablePayload(failure), node.runID, node.nodeID,
		transitionedAt, storage.ReasonFailureLeaseExpired)
	if err != nil {
		return 0, fmt.Errorf("fail exhausted node %q for run %q: %w", node.nodeID, node.runID, err)
	}

	transitioned, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect exhausted node recovery: %w", err)
	}

	if transitioned == 0 {
		return 0, nil
	}

	if cancelErr := cancelUnfinishedNodes(
		ctx, transaction, node.runID, storage.ReasonCanceledByRunFailure, transitionedAt,
	); cancelErr != nil {
		return 0, cancelErr
	}

	runResult, err := transaction.ExecContext(ctx, `UPDATE cord_runs
		SET status = 'failed', output_payload = NULL, error_payload = $1,
			updated_at = $3, completed_at = $3,
			terminal_reason = $4,
			terminal_runner_id = NULL
				WHERE id = $2 AND status = 'running'`, nullablePayload(failure), node.runID,
		transitionedAt, storage.ReasonFailureLeaseExpired)
	if err != nil {
		return 0, fmt.Errorf("fail run %q after exhausted lease: %w", node.runID, err)
	}

	if affectedErr := requireOneAffected(runResult, "exhausted lease run failure"); affectedErr != nil {
		return 0, affectedErr
	}

	return 1, nil
}
