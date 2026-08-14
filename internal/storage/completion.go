package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// LoadNodeInputs reads the root input or ordered parent outputs after claim.
func (s *Store) LoadNodeInputs(ctx context.Context, runID RunID, nodeID NodeID) (_ []EncodedPayload, err error) {
	rows, err := s.database.QueryContext(ctx, `SELECT p.output_payload FROM cord_edges AS e
		JOIN cord_nodes AS p ON p.run_id = e.run_id AND p.node_id = e.parent_node_id
		WHERE e.run_id = ? AND e.child_node_id = ? ORDER BY e.parent_order`, runID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("load parent outputs: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close parent outputs: %w", closeErr))
		}
	}()

	inputs := []EncodedPayload{}

	for rows.Next() {
		var payload []byte

		if scanErr := rows.Scan(&payload); scanErr != nil {
			return nil, fmt.Errorf("scan parent output: %w", scanErr)
		}

		inputs = append(inputs, EncodedPayload(payload))
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("iterate parent outputs: %w", rowsErr)
	}

	if len(inputs) > 0 {
		return inputs, nil
	}

	var payload []byte

	query := "SELECT input_payload FROM cord_runs WHERE id = ?"
	if scanErr := s.database.QueryRowContext(ctx, query, runID).Scan(&payload); scanErr != nil {
		return nil, fmt.Errorf("load run input: %w", scanErr)
	}

	return []EncodedPayload{EncodedPayload(payload)}, nil
}

// CompleteNode accepts a successful result only from the current, unexpired
// lease. It stores the output and releases child dependencies atomically.
func (s *Store) CompleteNode(
	ctx context.Context,
	runID RunID,
	nodeID NodeID,
	lease Lease,
	output EncodedPayload,
) (bool, error) {
	return s.fencedTerminalTransition(ctx, func(transaction *sql.Tx) error {
		result, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
			SET status = ?, output_payload = ?, error_payload = NULL,
				lease_owner = NULL, lease_expires_at = NULL,
				completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
			WHERE run_id = ? AND node_id = ? AND status = ?
				AND lease_owner = ? AND lease_generation = ?
				AND julianday(lease_expires_at) > julianday('now')
				AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)`,
			NodeCompleted, nullPayload(output), runID, nodeID, NodeRunning,
			lease.Owner, lease.Generation, runID, RunRunning)
		if err != nil {
			return fmt.Errorf("complete node %q for run %q: %w", nodeID, runID, err)
		}

		accepted, err := affectedOne(result)
		if err != nil {
			return fmt.Errorf("inspect node completion: %w", err)
		}

		if !accepted {
			return errFenceRejected
		}

		_, err = transaction.ExecContext(ctx, `UPDATE cord_nodes
			SET remaining_deps = remaining_deps - 1,
				status = CASE WHEN remaining_deps = 1 THEN ? ELSE status END
			WHERE run_id = ? AND status = ? AND remaining_deps > 0
				AND node_id IN (SELECT child_node_id FROM cord_edges
					WHERE run_id = ? AND parent_node_id = ?)`,
			NodeReady, runID, NodePending, runID, nodeID)
		if err != nil {
			return fmt.Errorf("release children of node %q for run %q: %w", nodeID, runID, err)
		}

		_, err = transaction.ExecContext(ctx, `UPDATE cord_runs
			SET status = ?, output_payload = ?, error_payload = NULL,
				updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
				completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
			WHERE id = ? AND status = ? AND terminal_node_id = ?`,
			RunCompleted, nullPayload(output), runID, RunRunning, nodeID)
		if err != nil {
			return fmt.Errorf("complete run %q: %w", runID, err)
		}

		return nil
	})
}

// ErrRunNotFound indicates that a requested run does not exist.
var ErrRunNotFound = errors.New("run not found")

// RunResult is the persistent terminal state observed by a waiter.
type RunResult struct {
	Status RunStatus
	Output EncodedPayload
	Error  EncodedPayload
}

// GetRunResult reads one run's current status and persistent result payloads.
func (s *Store) GetRunResult(ctx context.Context, runID RunID) (RunResult, error) {
	var (
		result          RunResult
		output, failure []byte
	)

	query := "SELECT status, output_payload, error_payload FROM cord_runs WHERE id = ?"
	if err := s.database.QueryRowContext(ctx, query, runID).Scan(
		&result.Status, &output, &failure,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, fmt.Errorf("read run result %q: %w", runID, ErrRunNotFound)
		}

		return result, fmt.Errorf("read run result: %w", err)
	}

	result.Output = EncodedPayload(output)
	result.Error = EncodedPayload(failure)

	return result, nil
}
