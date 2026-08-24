package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const claimQuery = `WITH registered(function_key, signature_hash) AS (
	SELECT function_key, signature_hash
	FROM jsonb_to_recordset($4::jsonb) AS registration(function_key text, signature_hash text)
), candidate AS (
	SELECT n.run_id, n.node_id
	FROM cord_nodes n
	JOIN cord_runs r ON r.id = n.run_id
	JOIN registered x
		ON x.function_key = n.function_key AND x.signature_hash = n.signature_hash
	WHERE n.status = 'ready'
		AND r.status = 'running'
		AND n.attempt < r.max_attempts
		AND n.available_at <= $3
	ORDER BY n.available_at, n.run_id, n.node_id
	FOR UPDATE OF n SKIP LOCKED
	LIMIT 1
)
UPDATE cord_nodes n
SET status = 'running',
	lease_owner = $1,
	lease_generation = n.lease_generation + 1,
	lease_expires_at = $3 + ($2 * interval '1 microsecond'),
	attempt = n.attempt + 1,
	started_at = COALESCE(n.started_at, $3),
	state_changed_at = CASE WHEN n.lifecycle_version IS NULL OR n.lifecycle_version = 1
		THEN $3 ELSE n.state_changed_at END,
	last_started_at = CASE WHEN n.lifecycle_version IS NULL OR n.lifecycle_version = 1
		THEN $3 ELSE n.last_started_at END,
	last_runner_id = CASE WHEN n.lifecycle_version IS NULL OR n.lifecycle_version = 1
		THEN $1 ELSE n.last_runner_id END,
	lifecycle_version = COALESCE(n.lifecycle_version, 1)
FROM candidate c, cord_runs r
WHERE n.run_id = c.run_id
	AND n.node_id = c.node_id
	AND r.id = n.run_id
RETURNING n.run_id, n.node_id, n.function_key, n.signature_hash, n.attempt,
	n.lease_generation, n.lease_expires_at,
	GREATEST(0, (EXTRACT(EPOCH FROM (n.lease_expires_at - $3)) * 1000000)::bigint),
	r.max_attempts, r.retry_base_delay_ns,
	r.retry_max_delay_ns, r.retry_policy_version`

// ClaimReadyNodeForFunctions claims one eligible node using PostgreSQL row locking.
func (s *Store) ClaimReadyNodeForFunctions(
	ctx context.Context,
	owner string,
	ttl time.Duration,
	registrations []storage.FunctionRegistration,
) (*storage.Claim, bool, error) {
	if owner == "" {
		return nil, false, errors.New("claim ready node: lease owner is empty")
	}

	if ttl <= 0 {
		return nil, false, errors.New("claim ready node: lease TTL must be positive")
	}

	if len(registrations) == 0 {
		return nil, false, nil
	}

	var claim *storage.Claim

	err := runTransaction(ctx, s.pool, "claim ready node", func(transaction *sql.Tx) error {
		claimedAt, timeErr := databaseInstant(ctx, transaction)
		if timeErr != nil {
			return timeErr
		}

		arguments, valuesErr := registrationArguments(owner, ttl, claimedAt, registrations)
		if valuesErr != nil {
			return valuesErr
		}

		claim, valuesErr = scanClaim(transaction.QueryRowContext(ctx, claimQuery, arguments...), owner)
		if errors.Is(valuesErr, sql.ErrNoRows) {
			claim = nil

			return nil
		}

		if valuesErr != nil {
			return fmt.Errorf("claim ready node: %w", valuesErr)
		}

		_, valuesErr = transaction.ExecContext(ctx, `UPDATE cord_runs
			SET started_at = COALESCE(started_at,
				(SELECT MIN(started_at) FROM cord_nodes WHERE run_id = $1)),
				lifecycle_version = COALESCE(lifecycle_version, 1)
			WHERE id = $1
				AND (started_at IS NULL OR lifecycle_version IS NULL)
				AND (lifecycle_version IS NULL OR lifecycle_version = 1)`, claim.RunID)
		if valuesErr != nil {
			return fmt.Errorf("record run start: %w", valuesErr)
		}

		return nil
	})
	if err != nil {
		return nil, false, err
	}

	return claim, claim != nil, nil
}

type registrationRecord struct {
	FunctionKey   string `json:"function_key"`
	SignatureHash string `json:"signature_hash"`
}

func registrationArguments(
	owner string,
	ttl time.Duration,
	claimedAt time.Time,
	registrations []storage.FunctionRegistration,
) ([]any, error) {
	records := make([]registrationRecord, 0, len(registrations))
	seen := make(map[string]struct{}, len(registrations))

	for _, registration := range registrations {
		if registration.Key == "" || registration.Signature == "" {
			return nil, errors.New("claim ready node: function registration is incomplete")
		}

		if _, exists := seen[registration.Key]; exists {
			return nil, fmt.Errorf(
				"claim ready node: duplicate function registration %q",
				registration.Key,
			)
		}

		seen[registration.Key] = struct{}{}
		records = append(records, registrationRecord{
			FunctionKey:   registration.Key,
			SignatureHash: registration.Signature,
		})
	}

	encoded, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("claim ready node: encode function registrations: %w", err)
	}

	return []any{owner, ttl.Microseconds(), claimedAt, string(encoded)}, nil
}

type rowScanner interface {
	Scan(destinations ...any) error
}

func scanClaim(row rowScanner, owner string) (*storage.Claim, error) {
	claim := &storage.Claim{}

	var baseDelay, maximumDelay, remainingMicros int64

	err := row.Scan(
		&claim.RunID,
		&claim.NodeID,
		&claim.FunctionKey,
		&claim.SignatureHash,
		&claim.Attempt,
		&claim.Lease.Generation,
		&claim.Lease.ExpiresAt,
		&remainingMicros,
		&claim.MaxAttempts,
		&baseDelay,
		&maximumDelay,
		&claim.RetryPolicyVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("scan claimed node: %w", err)
	}

	claim.Lease.Owner = owner
	claim.Lease.Remaining = time.Duration(remainingMicros) * time.Microsecond
	claim.RetryBaseDelay = time.Duration(baseDelay)
	claim.RetryMaxDelay = time.Duration(maximumDelay)

	return claim, nil
}
