package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Claim is a ready node claimed for execution and its fencing lease.
type Claim struct {
	RunID         RunID
	NodeID        NodeID
	FunctionKey   string
	SignatureHash string
	Lease         Lease
	Attempt       int
}

// ClaimReadyNode claims one currently eligible node. The boolean is false when
// no eligible node was won; callers may poll again later.
func (s *Store) ClaimReadyNode(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
) (_ *Claim, claimed bool, err error) {
	if owner == "" {
		return nil, false, errors.New("claim ready node: lease owner is empty")
	}

	if leaseTTL <= 0 {
		return nil, false, errors.New("claim ready node: lease TTL must be positive")
	}

	runID, nodeID, found, err := s.readyCandidate(ctx)
	if err != nil || !found {
		return nil, false, err
	}

	return s.claimCandidate(ctx, runID, nodeID, owner, leaseTTL)
}

func (s *Store) readyCandidate(ctx context.Context) (RunID, NodeID, bool, error) {
	var (
		runID  RunID
		nodeID NodeID
	)

	err := s.database.QueryRowContext(ctx, `SELECT n.run_id, n.node_id
		FROM cord_nodes AS n
		JOIN cord_runs AS r ON r.id = n.run_id
		WHERE n.status = ? AND r.status = ?
			AND julianday(n.available_at) <= julianday('now')
		ORDER BY julianday(n.available_at), n.run_id, n.node_id
		LIMIT 1`, NodeReady, RunRunning).Scan(&runID, &nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}

	if err != nil {
		return "", "", false, fmt.Errorf("select ready node: %w", err)
	}

	return runID, nodeID, true, nil
}

func (s *Store) claimCandidate(
	ctx context.Context,
	runID RunID,
	nodeID NodeID,
	owner string,
	leaseTTL time.Duration,
) (_ *Claim, claimed bool, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin ready-node claim: %w", err)
	}

	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback ready-node claim: %w", rollbackErr))
		}
	}()

	modifier := "+" + strconv.FormatFloat(leaseTTL.Seconds(), 'f', 6, 64) + " seconds"

	result, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
		SET status = ?, lease_owner = ?, lease_generation = lease_generation + 1,
			lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?),
			attempt = attempt + 1,
			started_at = COALESCE(started_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
		WHERE run_id = ? AND node_id = ? AND status = ?
			AND julianday(available_at) <= julianday('now')
			AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)`,
		NodeRunning, owner, modifier, runID, nodeID, NodeReady, runID, RunRunning)
	if err != nil {
		return nil, false, fmt.Errorf("claim ready node %q for run %q: %w", nodeID, runID, err)
	}

	won, err := affectedOne(result)
	if err != nil {
		return nil, false, fmt.Errorf("inspect ready-node claim: %w", err)
	}

	if !won {
		return nil, false, nil
	}

	claim := &Claim{
		Lease:         Lease{},
		RunID:         runID,
		NodeID:        nodeID,
		FunctionKey:   "",
		SignatureHash: "",
		Attempt:       0,
	}

	var expiresUnixMillis int64

	err = transaction.QueryRowContext(ctx, `SELECT function_key, signature_hash, attempt,
		lease_generation,
		CAST((julianday(lease_expires_at) - 2440587.5) * 86400000 AS INTEGER)
		FROM cord_nodes WHERE run_id = ? AND node_id = ?`, runID, nodeID).Scan(
		&claim.FunctionKey,
		&claim.SignatureHash,
		&claim.Attempt,
		&claim.Lease.Generation,
		&expiresUnixMillis,
	)
	if err != nil {
		return nil, false, fmt.Errorf("read claimed node %q for run %q: %w", nodeID, runID, err)
	}

	claim.Lease.Owner = owner
	claim.Lease.ExpiresAt = time.UnixMilli(expiresUnixMillis).UTC()

	if err = transaction.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit ready-node claim: %w", err)
	}

	return claim, true, nil
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

// CancelRun durably cancels a running or canceling run and all unfinished nodes.
// It returns false when the run is absent or already terminal.
func (s *Store) CancelRun(ctx context.Context, runID RunID) (accepted bool, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin run cancellation: %w", err)
	}

	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback run cancellation: %w", rollbackErr))
		}
	}()

	result, err := transaction.ExecContext(ctx, `UPDATE cord_runs SET status = ?,
		updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE id = ? AND status IN (?, ?)`, RunCanceling, runID, RunRunning, RunCanceling)
	if err != nil {
		return false, fmt.Errorf("request cancellation for run %q: %w", runID, err)
	}

	accepted, err = affectedOne(result)
	if err != nil {
		return false, fmt.Errorf("inspect run cancellation: %w", err)
	}

	if !accepted {
		return false, nil
	}

	cancelErr := cancelUnfinishedNodes(ctx, transaction, runID)
	if cancelErr != nil {
		return false, cancelErr
	}

	_, err = transaction.ExecContext(ctx, `UPDATE cord_runs SET status = ?,
		updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
		completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE id = ? AND status = ?`, RunCanceled, runID, RunCanceling)
	if err != nil {
		return false, fmt.Errorf("finish cancellation for run %q: %w", runID, err)
	}

	if err = transaction.Commit(); err != nil {
		return false, fmt.Errorf("commit run cancellation: %w", err)
	}

	return true, nil
}

var errFenceRejected = errors.New("lease fence rejected")

func (s *Store) fencedTerminalTransition(
	ctx context.Context,
	transition func(*sql.Tx) error,
) (accepted bool, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin fenced transition: %w", err)
	}

	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback fenced transition: %w", rollbackErr))
		}
	}()

	transitionErr := transition(transaction)
	if errors.Is(transitionErr, errFenceRejected) {
		return false, nil
	}

	if transitionErr != nil {
		return false, transitionErr
	}

	if err = transaction.Commit(); err != nil {
		return false, fmt.Errorf("commit fenced transition: %w", err)
	}

	return true, nil
}

func cancelUnfinishedNodes(ctx context.Context, transaction *sql.Tx, runID RunID) error {
	_, err := transaction.ExecContext(ctx, `UPDATE cord_nodes
		SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
			completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE run_id = ? AND status IN (?, ?, ?, ?)`,
		NodeCanceled, runID, NodePending, NodeReady, NodeRunning, NodeRetryWait)
	if err != nil {
		return fmt.Errorf("cancel unfinished nodes for run %q: %w", runID, err)
	}

	return nil
}

func affectedOne(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}

	return rows == 1, nil
}
