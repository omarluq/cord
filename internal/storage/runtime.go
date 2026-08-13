package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const expectedParentInputs = 2

// ClaimReadyNodeForFunctions claims work only when its exact function signature is registered.
func (s *Store) ClaimReadyNodeForFunctions(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
	registered map[string]string,
) (*Claim, bool, error) {
	if len(registered) == 0 {
		return nil, false, nil
	}

	registeredJSON, err := json.Marshal(registered)
	if err != nil {
		return nil, false, fmt.Errorf("encode registered functions: %w", err)
	}

	query := `SELECT n.run_id, n.node_id FROM cord_nodes AS n
		JOIN cord_runs AS r ON r.id = n.run_id
		JOIN json_each(?) AS registered
			ON registered.key = n.function_key AND registered.value = n.signature_hash
		WHERE n.status = ? AND r.status = ? AND julianday(n.available_at) <= julianday('now')
		ORDER BY julianday(n.available_at), n.run_id, n.node_id LIMIT 1`

	var (
		runID  RunID
		nodeID NodeID
	)

	err = s.database.QueryRowContext(
		ctx,
		query,
		registeredJSON,
		NodeReady,
		RunRunning,
	).Scan(&runID, &nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("select registered ready node: %w", err)
	}

	return s.claimCandidate(ctx, runID, nodeID, owner, leaseTTL)
}

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

	inputs := make([]EncodedPayload, 0, expectedParentInputs)

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

// RetryNode records one transient failure and releases its lease.
func (s *Store) RetryNode(
	ctx context.Context,
	runID RunID,
	nodeID NodeID,
	lease Lease,
	failure EncodedPayload,
	delay time.Duration,
) (bool, error) {
	modifier := "+" + strconv.FormatFloat(delay.Seconds(), 'f', 6, 64) + " seconds"

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

// RecoverExpiredLeases returns abandoned running nodes to ready with a newer fence.
func (s *Store) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	query := `UPDATE cord_nodes SET status = ?, lease_owner = NULL,
		lease_expires_at = NULL, lease_generation = lease_generation + 1 WHERE status = ?
		AND julianday(lease_expires_at) <= julianday('now')
		AND EXISTS (SELECT 1 FROM cord_runs WHERE id = run_id AND status = ?)`

	return s.updateNodes(ctx, query, "recover expired leases", NodeReady, NodeRunning, RunRunning)
}

func (s *Store) updateNodes(ctx context.Context, query, operation string, arguments ...any) (int64, error) {
	result, err := s.database.ExecContext(ctx, query, arguments...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", operation, err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect %s: %w", operation, err)
	}

	return count, nil
}

// HeartbeatNode extends an exact active lease using database time.
func (s *Store) HeartbeatNode(
	ctx context.Context,
	runID RunID,
	nodeID NodeID,
	lease Lease,
	ttl time.Duration,
) (bool, time.Time, error) {
	modifier := "+" + strconv.FormatFloat(ttl.Seconds(), 'f', 6, 64) + " seconds"

	result, err := s.database.ExecContext(ctx, `UPDATE cord_nodes
		SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
		WHERE run_id = ? AND node_id = ? AND status = ? AND lease_owner = ? AND lease_generation = ?
		AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)`, modifier,
		runID, nodeID, NodeRunning, lease.Owner, lease.Generation, runID, RunRunning)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("heartbeat node lease: %w", err)
	}

	accepted, err := affectedOne(result)
	if err != nil || !accepted {
		return accepted, time.Time{}, err
	}

	var millis int64

	query := `SELECT CAST((julianday(lease_expires_at) - 2440587.5) * 86400000 AS INTEGER)
		FROM cord_nodes WHERE run_id = ? AND node_id = ?`
	if scanErr := s.database.QueryRowContext(ctx, query, runID, nodeID).Scan(&millis); scanErr != nil {
		return false, time.Time{}, fmt.Errorf("read heartbeat expiration: %w", scanErr)
	}

	return true, time.UnixMilli(millis).UTC(), nil
}

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
		return result, fmt.Errorf("read run result: %w", err)
	}

	result.Output = EncodedPayload(output)
	result.Error = EncodedPayload(failure)

	return result, nil
}
