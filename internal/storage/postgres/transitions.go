package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const parentOutputsQuery = `SELECT parent.output_payload
	FROM cord_edges edge
	JOIN cord_nodes parent
		ON parent.run_id = edge.run_id AND parent.node_id = edge.parent_node_id
	WHERE edge.run_id = $1 AND edge.child_node_id = $2
	ORDER BY edge.parent_order`

// LoadNodeInputs loads ordered parent outputs, or the run input for a root node.
func (s *Store) LoadNodeInputs(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
) (_ []storage.EncodedPayload, err error) {
	rows, err := s.database.QueryContext(ctx, parentOutputsQuery, runID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("load parent outputs: %w", err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()

	inputs := make([]storage.EncodedPayload, 0)

	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan parent output: %w", err)
		}

		inputs = append(inputs, payload)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate parent outputs: %w", err)
	}

	if len(inputs) > 0 {
		return inputs, nil
	}

	const runInputQuery = `SELECT input_payload FROM cord_runs WHERE id = $1`

	var payload []byte
	if err = s.database.QueryRowContext(ctx, runInputQuery, runID).Scan(&payload); err != nil {
		return nil, fmt.Errorf("load run input: %w", err)
	}

	return []storage.EncodedPayload{payload}, nil
}

func (s *Store) terminal(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	payload storage.EncodedPayload,
	success bool,
) (bool, error) {
	retryCtx, cancel := leaseContext(ctx, lease.ExpiresAt)
	defer cancel()

	accepted := false

	err := runTransaction(retryCtx, s.database, "transition node", func(transaction *sql.Tx) error {
		accepted = false

		terminalNodeID, running, transitionErr := lockRunningRun(retryCtx, transaction, runID)
		if transitionErr != nil || !running {
			return transitionErr
		}

		transitioned, transitionErr := transitionNode(
			retryCtx, transaction, runID, nodeID, lease, payload, success,
		)
		if transitionErr != nil || !transitioned {
			return transitionErr
		}

		if success {
			transitionErr = completeRunPath(
				retryCtx, transaction, runID, nodeID, payload, nodeID == terminalNodeID,
			)
		} else {
			transitionErr = failRunPath(retryCtx, transaction, runID, payload)
		}

		if transitionErr != nil {
			return transitionErr
		}

		accepted = true

		return nil
	})
	if err != nil {
		return false, err
	}

	return accepted, nil
}

func lockRunningRun(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
) (storage.NodeID, bool, error) {
	const query = `SELECT terminal_node_id FROM cord_runs
		WHERE id = $1 AND status = 'running'
		FOR UPDATE`

	var terminalNodeID storage.NodeID
	if err := transaction.QueryRowContext(ctx, query, runID).Scan(&terminalNodeID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}

		return "", false, fmt.Errorf("lock running run: %w", err)
	}

	return terminalNodeID, true, nil
}

func transitionNode(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	payload storage.EncodedPayload,
	success bool,
) (bool, error) {
	const query = `UPDATE cord_nodes
		SET status = $1,
			output_payload = $2,
			error_payload = $3,
			lease_owner = NULL,
			lease_expires_at = NULL,
			completed_at = clock_timestamp()
		WHERE run_id = $4
			AND node_id = $5
			AND status = 'running'
			AND lease_owner = $6
			AND lease_generation = $7
			AND lease_expires_at > clock_timestamp()
			AND EXISTS (
				SELECT 1 FROM cord_runs WHERE id = $4 AND status = 'running'
			)`

	nodeStatus := storage.NodeFailed

	output, failure := any(nil), nullablePayload(payload)
	if success {
		nodeStatus, output, failure = storage.NodeCompleted, nullablePayload(payload), nil
	}

	result, err := transaction.ExecContext(
		ctx, query, nodeStatus, output, failure, runID, nodeID, lease.Owner, lease.Generation,
	)
	if err != nil {
		return false, fmt.Errorf("transition node state: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect node transition: %w", err)
	}

	return count == 1, nil
}

func completeRunPath(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	nodeID storage.NodeID,
	payload storage.EncodedPayload,
	terminal bool,
) error {
	const releaseChildren = `UPDATE cord_nodes
		SET remaining_deps = remaining_deps - 1,
			status = CASE WHEN remaining_deps = 1 THEN 'ready' ELSE status END,
			available_at = CASE
				WHEN remaining_deps = 1 THEN clock_timestamp() ELSE available_at
			END
		WHERE run_id = $1
			AND status = 'pending'
			AND remaining_deps > 0
			AND node_id IN (
				SELECT child_node_id FROM cord_edges
				WHERE run_id = $1 AND parent_node_id = $2
			)`
	if _, err := transaction.ExecContext(ctx, releaseChildren, runID, nodeID); err != nil {
		return fmt.Errorf("release child nodes: %w", err)
	}

	if !terminal {
		return nil
	}

	const completeRun = `UPDATE cord_runs
		SET status = $1,
			output_payload = $2,
			error_payload = NULL,
			updated_at = clock_timestamp(),
			completed_at = clock_timestamp()
		WHERE id = $3 AND status = 'running' AND terminal_node_id = $4`

	result, err := transaction.ExecContext(
		ctx, completeRun, storage.RunCompleted, nullablePayload(payload), runID, nodeID,
	)
	if err != nil {
		return fmt.Errorf("complete run: %w", err)
	}

	if affectedErr := requireOneAffected(result, "run completion"); affectedErr != nil {
		return affectedErr
	}

	return cancelUnfinishedNodes(ctx, transaction, runID)
}

func failRunPath(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	payload storage.EncodedPayload,
) error {
	if err := cancelUnfinishedNodes(ctx, transaction, runID); err != nil {
		return err
	}

	const failRun = `UPDATE cord_runs
		SET status = $1,
			output_payload = NULL,
			error_payload = $2,
			updated_at = clock_timestamp(),
			completed_at = clock_timestamp()
		WHERE id = $3 AND status = 'running'`

	result, err := transaction.ExecContext(
		ctx, failRun, storage.RunFailed, nullablePayload(payload), runID,
	)
	if err != nil {
		return fmt.Errorf("fail run: %w", err)
	}

	return requireOneAffected(result, "run failure")
}

func cancelUnfinishedNodes(ctx context.Context, transaction *sql.Tx, runID storage.RunID) error {
	const query = `UPDATE cord_nodes
		SET status = 'canceled',
			lease_owner = NULL,
			lease_expires_at = NULL,
			completed_at = clock_timestamp()
		WHERE run_id = $1 AND status IN ('pending', 'ready', 'running', 'retry_wait')`
	if _, err := transaction.ExecContext(ctx, query, runID); err != nil {
		return fmt.Errorf("cancel unfinished nodes: %w", err)
	}

	return nil
}

func requireOneAffected(result sql.Result, operation string) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect %s: %w", operation, err)
	}

	if count != 1 {
		return fmt.Errorf("%s affected %d rows, expected 1", operation, count)
	}

	return nil
}

// CompleteNode records a successful result under an active lease.
func (s *Store) CompleteNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	payload storage.EncodedPayload,
) (bool, error) {
	return s.terminal(ctx, runID, nodeID, lease, payload, true)
}

// FailNode records a terminal failure under an active lease.
func (s *Store) FailNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	payload storage.EncodedPayload,
) (bool, error) {
	return s.terminal(ctx, runID, nodeID, lease, payload, false)
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
			available_at = clock_timestamp() + ($2 * interval '1 microsecond'),
			lease_owner = NULL,
			lease_expires_at = NULL
		WHERE run_id = $3
			AND node_id = $4
			AND status = 'running'
			AND lease_owner = $5
			AND lease_generation = $6
			AND lease_expires_at > clock_timestamp()
			AND EXISTS (
				SELECT 1 FROM cord_runs WHERE id = $3 AND status = 'running'
			)`

	retryCtx, cancel := leaseContext(ctx, lease.ExpiresAt)
	defer cancel()

	accepted := false
	err := runOperation(retryCtx, "schedule node retry", func() error {
		accepted = false

		result, execErr := s.database.ExecContext(
			retryCtx,
			query,
			nullablePayload(payload),
			delay.Microseconds(),
			runID,
			nodeID,
			lease.Owner,
			lease.Generation,
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
func (s *Store) PromoteRetries(ctx context.Context) (int64, error) {
	const query = `UPDATE cord_nodes SET status = 'ready'
		WHERE status = 'retry_wait'
			AND available_at <= clock_timestamp()
			AND EXISTS (
				SELECT 1 FROM cord_runs WHERE id = run_id AND status = 'running'
			)`

	return s.update(ctx, query, "promote retries")
}

// RecoverExpiredLeases returns abandoned nodes to ready with a newer fence.
func (s *Store) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	const query = `UPDATE cord_nodes
		SET status = 'ready',
			lease_owner = NULL,
			lease_expires_at = NULL,
			lease_generation = lease_generation + 1
		WHERE status = 'running'
			AND lease_expires_at <= clock_timestamp()
			AND EXISTS (
				SELECT 1 FROM cord_runs WHERE id = run_id AND status = 'running'
			)`

	return s.update(ctx, query, "recover expired leases")
}

func (s *Store) update(ctx context.Context, query, operation string) (int64, error) {
	var count int64

	err := runOperation(ctx, operation, func() error {
		result, execErr := s.database.ExecContext(ctx, query)
		if execErr != nil {
			return fmt.Errorf("execute %s: %w", operation, execErr)
		}

		var rowsErr error

		count, rowsErr = result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("inspect %s: %w", operation, rowsErr)
		}

		return nil
	})

	return count, err
}

// HeartbeatNode extends an exact active lease using database time.
func (s *Store) HeartbeatNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	ttl time.Duration,
) (bool, time.Time, error) {
	if ttl <= 0 {
		return false, time.Time{}, errors.New("heartbeat node lease: TTL must be positive")
	}

	const query = `UPDATE cord_nodes
		SET lease_expires_at = clock_timestamp() + ($1 * interval '1 microsecond')
		WHERE run_id = $2
			AND node_id = $3
			AND status = 'running'
			AND lease_owner = $4
			AND lease_generation = $5
			AND lease_expires_at > clock_timestamp()
			AND EXISTS (
				SELECT 1 FROM cord_runs WHERE id = $2 AND status = 'running'
			)
		RETURNING lease_expires_at`

	retryCtx, cancel := leaseContext(ctx, lease.ExpiresAt)
	defer cancel()

	var expiry time.Time

	accepted := false

	err := runOperation(retryCtx, "heartbeat node lease", func() error {
		accepted = false
		expiry = time.Time{}

		scanErr := s.database.QueryRowContext(
			retryCtx, query, ttl.Microseconds(), runID, nodeID, lease.Owner, lease.Generation,
		).Scan(&expiry)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}

		if scanErr != nil {
			return fmt.Errorf("update heartbeat: %w", scanErr)
		}

		accepted = true

		return nil
	})

	return accepted, expiry, err
}

// GetRunResult returns persisted run state and payloads.
func (s *Store) GetRunResult(
	ctx context.Context,
	runID storage.RunID,
) (storage.RunResult, error) {
	const query = `SELECT status, output_payload, error_payload FROM cord_runs WHERE id = $1`

	var (
		result          storage.RunResult
		output, failure []byte
	)

	err := s.database.QueryRowContext(ctx, query, runID).Scan(&result.Status, &output, &failure)
	if errors.Is(err, sql.ErrNoRows) {
		return result, fmt.Errorf("read run result %q: %w", runID, storage.ErrRunNotFound)
	}

	if err != nil {
		return result, fmt.Errorf("read run result: %w", err)
	}

	result.Output, result.Error = output, failure

	return result, nil
}
