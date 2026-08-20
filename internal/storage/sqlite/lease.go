package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	"strconv"
	"time"
)

// RecoverExpiredLeases returns abandoned running nodes to ready with a newer fence.
func (s *Store) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	query := `UPDATE cord_nodes SET status = ?, lease_owner = NULL,
		lease_expires_at = NULL, lease_generation = lease_generation + 1 WHERE status = ?
		AND julianday(lease_expires_at) <= julianday('now')
		AND EXISTS (SELECT 1 FROM cord_runs WHERE id = run_id AND status = ?)`

	return s.updateNodes(
		ctx, query, "recover expired leases", storage.NodeReady, storage.NodeRunning, storage.RunRunning,
	)
}

// HeartbeatNode extends an exact active lease using database time.
func (s *Store) HeartbeatNode(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	ttl time.Duration,
) (bool, time.Time, error) {
	if ttl <= 0 {
		return false, time.Time{}, errors.New("heartbeat node lease: TTL must be positive")
	}

	modifier := durationModifier(ttl)

	var millis int64

	accepted := false

	err := retryFencedContention(ctx, "retry node heartbeat", lease.ExpiresAt, func(attemptCtx context.Context) error {
		scanErr := s.database.QueryRowContext(attemptCtx, `UPDATE cord_nodes
			SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
			WHERE run_id = ? AND node_id = ? AND status = ? AND lease_owner = ? AND lease_generation = ?
			AND julianday(lease_expires_at) > julianday('now')
			AND EXISTS (SELECT 1 FROM cord_runs WHERE id = ? AND status = ?)
			RETURNING CAST((julianday(lease_expires_at) - 2440587.5) * 86400000 AS INTEGER)`, modifier,
			runID, nodeID, storage.NodeRunning, lease.Owner, lease.Generation, runID, storage.RunRunning).Scan(&millis)
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
		return false, time.Time{}, err
	}

	if !accepted {
		return false, time.Time{}, nil
	}

	return true, time.UnixMilli(millis).UTC(), nil
}

func durationModifier(duration time.Duration) string {
	seconds := strconv.FormatFloat(duration.Seconds(), 'f', 6, 64)
	if duration >= 0 {
		seconds = "+" + seconds
	}

	return seconds + " seconds"
}
