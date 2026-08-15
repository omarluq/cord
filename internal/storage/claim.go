package storage

import (
	"context"
	"database/sql"
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

const (
	claimReadyNodePrefix = `UPDATE cord_nodes
		SET status = ?, lease_owner = ?, lease_generation = lease_generation + 1,
			lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?),
			attempt = attempt + 1,
			started_at = COALESCE(started_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
		WHERE (run_id, node_id) = (
			SELECT n.run_id, n.node_id
			FROM cord_nodes AS n
			JOIN cord_runs AS r ON r.id = n.run_id`
	claimReadyNodeSuffix = `
			WHERE n.status = ? AND r.status = ? AND n.attempt < r.max_attempts
				AND julianday(n.available_at) <= julianday('now')
			ORDER BY julianday(n.available_at), n.run_id, n.node_id
			LIMIT 1
		)
		RETURNING run_id, node_id, function_key, signature_hash, attempt,
			lease_generation,
			CAST((julianday(lease_expires_at) - 2440587.5) * 86400000 AS INTEGER),
			(SELECT max_attempts FROM cord_runs WHERE id = run_id),
			(SELECT retry_base_delay_ns FROM cord_runs WHERE id = run_id),
			(SELECT retry_max_delay_ns FROM cord_runs WHERE id = run_id),
			(SELECT retry_policy_version FROM cord_runs WHERE id = run_id)`

	claimReadyNodeStatement           = claimReadyNodePrefix + claimReadyNodeSuffix
	claimRegisteredReadyNodeStatement = claimReadyNodePrefix + `
			JOIN json_each(?) AS registered
				ON registered.key = n.function_key AND registered.value = n.signature_hash` + claimReadyNodeSuffix
)

// ClaimReadyNode claims one currently eligible node. The boolean is false when
// no eligible node was won; callers may poll again later.
func (s *Store) ClaimReadyNode(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
) (*Claim, bool, error) {
	if err := validateClaimLease(owner, leaseTTL); err != nil {
		return nil, false, err
	}

	return s.claimReadyNode(ctx, owner, leaseTTL, nil)
}

// ClaimReadyNodeForFunctions claims work only when its exact function signature is registered.
func (s *Store) ClaimReadyNodeForFunctions(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
	registeredJSON []byte,
) (*Claim, bool, error) {
	if err := validateClaimLease(owner, leaseTTL); err != nil {
		return nil, false, err
	}

	if len(registeredJSON) == 0 {
		return nil, false, nil
	}

	return s.claimReadyNode(ctx, owner, leaseTTL, registeredJSON)
}

func (s *Store) claimReadyNode(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
	registeredJSON []byte,
) (claim *Claim, claimed bool, err error) {
	err = retrySQLiteContention(ctx, "retry ready-node claim", func() error {
		claim, claimed, err = s.claimReadyNodeOnce(ctx, owner, leaseTTL, registeredJSON)

		return err
	})

	return claim, claimed, err
}

func (s *Store) claimReadyNodeOnce(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
	registeredJSON []byte,
) (*Claim, bool, error) {
	statement := claimReadyNodeStatement
	arguments := []any{NodeRunning, owner, sqliteDurationModifier(leaseTTL), NodeReady, RunRunning}

	if len(registeredJSON) > 0 {
		statement = claimRegisteredReadyNodeStatement
		arguments = []any{NodeRunning, owner, sqliteDurationModifier(leaseTTL), registeredJSON, NodeReady, RunRunning}
	}

	claim, err := scanClaim(s.database.QueryRowContext(ctx, statement, arguments...), owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("claim ready node: %w", err)
	}

	return claim, true, nil
}

func scanClaim(row *sql.Row, owner string) (*Claim, error) {
	claim := &Claim{}

	var expiresUnixMillis, retryBaseDelayNS, retryMaxDelayNS int64

	err := row.Scan(
		&claim.RunID,
		&claim.NodeID,
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
		return nil, fmt.Errorf("scan claimed node: %w", err)
	}

	claim.Lease.Owner = owner
	claim.Lease.ExpiresAt = time.UnixMilli(expiresUnixMillis).UTC()
	claim.RetryBaseDelay = time.Duration(retryBaseDelayNS)
	claim.RetryMaxDelay = time.Duration(retryMaxDelayNS)

	return claim, nil
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
