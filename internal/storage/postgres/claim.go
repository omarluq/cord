package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const claimQuery = `WITH registered(function_key, signature_hash) AS (VALUES %s), candidate AS (
	SELECT n.run_id, n.node_id
	FROM cord_nodes n
	JOIN cord_runs r ON r.id = n.run_id
	JOIN registered x
		ON x.function_key = n.function_key AND x.signature_hash = n.signature_hash
	WHERE n.status = 'ready'
		AND r.status = 'running'
		AND n.attempt < r.max_attempts
		AND n.available_at <= clock_timestamp()
	ORDER BY n.available_at, n.run_id, n.node_id
	FOR UPDATE OF n SKIP LOCKED
	LIMIT 1
)
UPDATE cord_nodes n
SET status = 'running',
	lease_owner = $1,
	lease_generation = n.lease_generation + 1,
	lease_expires_at = clock_timestamp() + ($2 * interval '1 microsecond'),
	attempt = n.attempt + 1,
	started_at = COALESCE(n.started_at, clock_timestamp())
FROM candidate c, cord_runs r
WHERE n.run_id = c.run_id
	AND n.node_id = c.node_id
	AND r.id = n.run_id
RETURNING n.run_id, n.node_id, n.function_key, n.signature_hash, n.attempt,
	n.lease_generation, n.lease_expires_at, r.max_attempts, r.retry_base_delay_ns,
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

	values, arguments, err := registrationValues(owner, ttl, registrations)
	if err != nil {
		return nil, false, err
	}

	claim, err := scanClaim(s.database.QueryRowContext(ctx, fmt.Sprintf(claimQuery, values), arguments...), owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("claim ready node: %w", err)
	}

	return claim, true, nil
}

func registrationValues(
	owner string,
	ttl time.Duration,
	registrations []storage.FunctionRegistration,
) (valueList string, arguments []any, err error) {
	arguments = []any{owner, ttl.Microseconds()}
	values := make([]string, 0, len(registrations))
	seen := make(map[string]struct{}, len(registrations))

	for _, registration := range registrations {
		if registration.Key == "" || registration.Signature == "" {
			return "", nil, errors.New("claim ready node: function registration is incomplete")
		}

		if _, exists := seen[registration.Key]; exists {
			return "", nil, fmt.Errorf(
				"claim ready node: duplicate function registration %q",
				registration.Key,
			)
		}

		seen[registration.Key] = struct{}{}
		arguments = append(arguments, registration.Key, registration.Signature)
		position := len(arguments) - 1
		values = append(values, fmt.Sprintf("($%d,$%d)", position, position+1))
	}

	return strings.Join(values, ","), arguments, nil
}

type rowScanner interface {
	Scan(destinations ...any) error
}

func scanClaim(row rowScanner, owner string) (*storage.Claim, error) {
	claim := &storage.Claim{}

	var baseDelay, maximumDelay int64

	err := row.Scan(
		&claim.RunID,
		&claim.NodeID,
		&claim.FunctionKey,
		&claim.SignatureHash,
		&claim.Attempt,
		&claim.Lease.Generation,
		&claim.Lease.ExpiresAt,
		&claim.MaxAttempts,
		&baseDelay,
		&maximumDelay,
		&claim.RetryPolicyVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("scan claimed node: %w", err)
	}

	claim.Lease.Owner = owner
	claim.RetryBaseDelay = time.Duration(baseDelay)
	claim.RetryMaxDelay = time.Duration(maximumDelay)

	return claim, nil
}
