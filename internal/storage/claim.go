package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Claim is a ready node claimed for execution and its fencing lease.
type Claim struct {
	RunID              RunID
	NodeID             NodeID
	FunctionKey        string
	SignatureHash      string
	Lease              Lease
	Attempt            int
	MaxAttempts        int
	RetryBaseDelay     time.Duration
	RetryMaxDelay      time.Duration
	RetryPolicyVersion int
}

const maxClaimAttempts = 3

// ClaimReadyNode claims one currently eligible node. The boolean is false when
// no eligible node was won; callers may poll again later.
func (s *Store) ClaimReadyNode(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
) (_ *Claim, claimed bool, err error) {
	if validationErr := validateClaimLease(owner, leaseTTL); validationErr != nil {
		return nil, false, validationErr
	}

	return s.claimReadyCandidate(ctx, owner, leaseTTL, func() (RunID, NodeID, bool, error) {
		return s.readyCandidate(ctx)
	})
}

func (s *Store) claimReadyCandidate(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
	selectCandidate func() (RunID, NodeID, bool, error),
) (*Claim, bool, error) {
	for range maxClaimAttempts {
		runID, nodeID, found, err := selectCandidate()
		if err != nil || !found {
			return nil, false, err
		}

		claim, claimed, err := s.claimCandidate(ctx, runID, nodeID, owner, leaseTTL)
		if err != nil || claimed {
			return claim, claimed, err
		}
	}

	return nil, false, nil
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

	modifier := sqliteDurationModifier(leaseTTL)

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

	claim, err := readClaim(ctx, transaction, runID, nodeID, owner)
	if err != nil {
		return nil, false, err
	}

	if err = transaction.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit ready-node claim: %w", err)
	}

	return claim, true, nil
}

func readClaim(
	ctx context.Context,
	transaction *sql.Tx,
	runID RunID,
	nodeID NodeID,
	owner string,
) (*Claim, error) {
	claim := &Claim{
		RunID:              runID,
		NodeID:             nodeID,
		FunctionKey:        "",
		SignatureHash:      "",
		Lease:              Lease{},
		Attempt:            0,
		MaxAttempts:        0,
		RetryBaseDelay:     0,
		RetryMaxDelay:      0,
		RetryPolicyVersion: 0,
	}

	var expiresUnixMillis, retryBaseDelayNS, retryMaxDelayNS int64

	err := transaction.QueryRowContext(ctx, `SELECT n.function_key, n.signature_hash, n.attempt,
		n.lease_generation,
		CAST((julianday(n.lease_expires_at) - 2440587.5) * 86400000 AS INTEGER),
		r.max_attempts, r.retry_base_delay_ns, r.retry_max_delay_ns, r.retry_policy_version
		FROM cord_nodes AS n
		JOIN cord_runs AS r ON r.id = n.run_id
		WHERE n.run_id = ? AND n.node_id = ?`, runID, nodeID).Scan(
		&claim.FunctionKey,
		&claim.SignatureHash,
		&claim.Attempt,
		&claim.Lease.Generation,
		&expiresUnixMillis,
		&claim.MaxAttempts,
		&retryBaseDelayNS,
		&retryMaxDelayNS,
		&claim.RetryPolicyVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("read claimed node %q for run %q: %w", nodeID, runID, err)
	}

	claim.Lease.Owner = owner
	claim.Lease.ExpiresAt = time.UnixMilli(expiresUnixMillis).UTC()
	claim.RetryBaseDelay = time.Duration(retryBaseDelayNS)
	claim.RetryMaxDelay = time.Duration(retryMaxDelayNS)

	return claim, nil
}

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

	if err := validateClaimLease(owner, leaseTTL); err != nil {
		return nil, false, err
	}

	registeredJSON, err := json.Marshal(registered)
	if err != nil {
		return nil, false, fmt.Errorf("encode registered functions: %w", err)
	}

	return s.claimReadyCandidate(ctx, owner, leaseTTL, func() (RunID, NodeID, bool, error) {
		return s.registeredReadyCandidate(ctx, registeredJSON)
	})
}

func (s *Store) registeredReadyCandidate(
	ctx context.Context,
	registeredJSON []byte,
) (RunID, NodeID, bool, error) {
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

	err := s.database.QueryRowContext(
		ctx,
		query,
		registeredJSON,
		NodeReady,
		RunRunning,
	).Scan(&runID, &nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}

	if err != nil {
		return "", "", false, fmt.Errorf("select registered ready node: %w", err)
	}

	return runID, nodeID, true, nil
}

func validateClaimLease(owner string, leaseTTL time.Duration) error {
	if owner == "" {
		return errors.New("claim ready node: lease owner is empty")
	}

	if leaseTTL <= 0 {
		return errors.New("claim ready node: lease TTL must be positive")
	}

	return nil
}
