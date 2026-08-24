package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const (
	claimReadyNodePrefix = `UPDATE cord_nodes
		SET status = ?, lease_owner = ?, lease_generation = lease_generation + 1,
			lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?),
			attempt = attempt + 1,
			started_at = COALESCE(started_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
			state_changed_at = CASE WHEN lifecycle_version IS NULL OR lifecycle_version = 1
				THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE state_changed_at END,
			last_started_at = CASE WHEN lifecycle_version IS NULL OR lifecycle_version = 1
				THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE last_started_at END,
			last_runner_id = CASE WHEN lifecycle_version IS NULL OR lifecycle_version = 1
				THEN ? ELSE last_runner_id END,
			lifecycle_version = COALESCE(lifecycle_version, 1)
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
			MAX(0, CAST((julianday(lease_expires_at) - julianday('now')) * 86400000000 AS INTEGER)),
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
) (*storage.Claim, bool, error) {
	if err := validateClaimLease(owner, leaseTTL); err != nil {
		return nil, false, err
	}

	return s.claimReadyNode(ctx, owner, leaseTTL, nil)
}

// ClaimReadyNodeForFunctions claims work only when its exact function signature is registered.
// ClaimReadyNodeForFunctions reports no claim when registrations is empty because no signature can match.
func (s *Store) ClaimReadyNodeForFunctions(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
	registrations []storage.FunctionRegistration,
) (*storage.Claim, bool, error) {
	if err := validateClaimLease(owner, leaseTTL); err != nil {
		return nil, false, err
	}

	registeredJSON, err := encodeRegistrations(registrations)
	if err != nil || len(registeredJSON) == 0 {
		return nil, false, err
	}

	return s.claimReadyNode(ctx, owner, leaseTTL, registeredJSON)
}

func (s *Store) claimReadyNode(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
	registeredJSON []byte,
) (claim *storage.Claim, claimed bool, err error) {
	err = retryContention(ctx, "retry ready-node claim", func(attemptCtx context.Context) error {
		claim, claimed, err = s.claimReadyNodeOnce(attemptCtx, owner, leaseTTL, registeredJSON)

		return err
	})

	return claim, claimed, err
}

func (s *Store) claimReadyNodeOnce(
	ctx context.Context,
	owner string,
	leaseTTL time.Duration,
	registeredJSON []byte,
) (_ *storage.Claim, claimed bool, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin ready-node claim: %w", err)
	}
	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback ready-node claim: %w", rollbackErr))
		}
	}()

	statement := claimReadyNodeStatement
	arguments := []any{
		storage.NodeRunning, owner, durationModifier(leaseTTL), owner,
		storage.NodeReady, storage.RunRunning,
	}

	if len(registeredJSON) > 0 {
		statement = claimRegisteredReadyNodeStatement
		arguments = []any{
			storage.NodeRunning, owner, durationModifier(leaseTTL), owner,
			registeredJSON, storage.NodeReady, storage.RunRunning,
		}
	}

	claim, err := scanClaim(transaction.QueryRowContext(ctx, statement, arguments...), owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("claim ready node: %w", err)
	}

	if _, err = transaction.ExecContext(ctx, `UPDATE cord_runs
		SET started_at = COALESCE(started_at,
			(SELECT MIN(started_at) FROM cord_nodes WHERE run_id = ?)),
			lifecycle_version = COALESCE(lifecycle_version, ?)
		WHERE id = ? AND (started_at IS NULL OR lifecycle_version IS NULL)`,
		claim.RunID, storage.LifecycleVersion1, claim.RunID); err != nil {
		return nil, false, fmt.Errorf("record run start: %w", err)
	}

	if err = transaction.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit ready-node claim: %w", err)
	}

	return claim, true, nil
}

func scanClaim(row *sql.Row, owner string) (*storage.Claim, error) {
	claim := &storage.Claim{}

	var expiresUnixMillis, remainingMicros, retryBaseDelayNS, retryMaxDelayNS int64

	err := row.Scan(
		&claim.RunID, &claim.NodeID, &claim.FunctionKey, &claim.SignatureHash,
		&claim.Attempt, &claim.Lease.Generation, &expiresUnixMillis, &remainingMicros,
		&claim.MaxAttempts, &retryBaseDelayNS, &retryMaxDelayNS, &claim.RetryPolicyVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("scan claimed node: %w", err)
	}

	claim.Lease.Owner = owner
	claim.Lease.ExpiresAt = time.UnixMilli(expiresUnixMillis).UTC()
	claim.Lease.Remaining = time.Duration(remainingMicros) * time.Microsecond
	claim.RetryBaseDelay = time.Duration(retryBaseDelayNS)
	claim.RetryMaxDelay = time.Duration(retryMaxDelayNS)

	return claim, nil
}

func encodeRegistrations(registrations []storage.FunctionRegistration) ([]byte, error) {
	if len(registrations) == 0 {
		return nil, nil
	}

	values := make(map[string]string, len(registrations))
	for _, registration := range registrations {
		if registration.Key == "" || registration.Signature == "" {
			return nil, errors.New("claim ready node: function registration is incomplete")
		}

		if _, exists := values[registration.Key]; exists {
			return nil, fmt.Errorf("claim ready node: duplicate function registration %q", registration.Key)
		}

		values[registration.Key] = registration.Signature
	}

	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("claim ready node: encode function registrations: %w", err)
	}

	return encoded, nil
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
