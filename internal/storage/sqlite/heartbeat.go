package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// HeartbeatNode extends an exact active lease using database time and returns
// its database-relative remaining lifetime.
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

	modifier := durationModifier(ttl)

	var remainingMicros int64

	accepted := false

	err := retryFencedContention(ctx, "retry node heartbeat", lease.Remaining, func(attemptCtx context.Context) error {
		scanErr := s.database.QueryRowContext(attemptCtx, `UPDATE cord_nodes
			SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
			WHERE run_id = ? AND node_id = ? AND status = ? AND lease_owner = ? AND lease_generation = ?
			AND julianday(lease_expires_at) > julianday('now')
			AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)
			RETURNING MAX(0, CAST((julianday(lease_expires_at) - julianday('now')) * 86400000000 AS INTEGER))`,
			modifier, runID, nodeID, storage.NodeRunning, lease.Owner, lease.Generation,
			runID, storage.RunRunning).Scan(&remainingMicros)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}

		if scanErr != nil {
			return fmt.Errorf("heartbeat node lease: %w", scanErr)
		}

		accepted = true

		return nil
	})
	if err != nil {
		return false, 0, err
	}

	if !accepted {
		return false, 0, nil
	}

	return true, time.Duration(remainingMicros) * time.Microsecond, nil
}

func durationModifier(duration time.Duration) string {
	seconds := strconv.FormatFloat(duration.Seconds(), 'f', 6, 64)
	if duration >= 0 {
		seconds = "+" + seconds
	}

	return seconds + " seconds"
}
