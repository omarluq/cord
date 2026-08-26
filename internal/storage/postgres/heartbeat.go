package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// HeartbeatNode extends an exact active lease using database time.
func (s *Store) HeartbeatNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	ttl time.Duration,
) (bool, time.Duration, error) {
	if ttl <= 0 {
		return false, 0, errors.New("heartbeat node lease: TTL must be positive")
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
		RETURNING GREATEST(0,
			(EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp())) * 1000000)::bigint)`

	retryCtx, cancel := leaseContext(ctx, lease.Remaining)
	defer cancel()

	var remainingMicros int64

	accepted := false

	err := runOperation(retryCtx, "heartbeat node lease", func() error {
		accepted = false
		remainingMicros = 0

		scanErr := s.pool.QueryRowContext(
			retryCtx, query, ttl.Microseconds(), runID, nodeID, lease.Owner, lease.Generation,
		).Scan(&remainingMicros)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}

		if scanErr != nil {
			return fmt.Errorf("update heartbeat: %w", scanErr)
		}

		accepted = true

		return nil
	})

	return accepted, time.Duration(remainingMicros) * time.Microsecond, err
}
