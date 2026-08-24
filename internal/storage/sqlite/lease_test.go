package sqlite_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_HeartbeatNodeRejectsNonPositiveTTLWithoutMutation(t *testing.T) {
	t.Parallel()

	for _, ttl := range []time.Duration{0, -time.Second} {
		t.Run(ttl.String(), func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("heartbeat-ttl-"+ttl.String()))
			requireCreateRun(t.Context(), t, store, &plan)
			claim := claimNode(t, store)

			var before string

			err := database.QueryRowContext(t.Context(), `SELECT lease_expires_at FROM cord_nodes
				WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID).Scan(&before)
			require.NoError(t, err)

			accepted, expiry, err := store.HeartbeatNode(
				t.Context(), claim.RunID, claim.NodeID, claim.Lease, ttl,
			)
			require.ErrorContains(t, err, "TTL must be positive")
			assert.False(t, accepted)
			assert.Zero(t, expiry)

			var after string

			err = database.QueryRowContext(t.Context(), `SELECT lease_expires_at FROM cord_nodes
				WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID).Scan(&after)
			require.NoError(t, err)
			assert.Equal(t, before, after)
		})
	}
}

func TestStore_HeartbeatNodeRejectsExpiredLeaseWithoutMutation(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "heartbeat-expired")
	requireCreateRun(t.Context(), t, store, &plan)
	claim := claimNode(t, store)

	const expiredAt = "2000-01-01T00:00:00.000Z"

	_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes SET lease_expires_at = ?
		WHERE run_id = ? AND node_id = ?`, expiredAt, claim.RunID, claim.NodeID)
	require.NoError(t, err)

	accepted, expiry, err := store.HeartbeatNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, time.Minute,
	)
	require.NoError(t, err)
	assert.False(t, accepted)
	assert.Zero(t, expiry)

	var persistedExpiry string

	err = database.QueryRowContext(t.Context(), `SELECT lease_expires_at FROM cord_nodes
		WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID).Scan(&persistedExpiry)
	require.NoError(t, err)
	assert.Equal(t, expiredAt, persistedExpiry)
}

func TestStore_HeartbeatNodeRejectsInactiveRun(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "heartbeat-canceled")
	requireCreateRun(t.Context(), t, store, &plan)
	claim := claimNode(t, store)

	accepted, err := sqlite.CancelRunForTest(t.Context(), store, claim.RunID)
	require.NoError(t, err)
	require.True(t, accepted)

	heartbeatAccepted, expiry, err := store.HeartbeatNode(
		t.Context(),
		claim.RunID,
		claim.NodeID,
		claim.Lease,
		time.Minute,
	)
	require.NoError(t, err)
	assert.False(t, heartbeatAccepted)
	assert.Zero(t, expiry)
}

func TestStore_RecoverExpiredLeasesLeavesActiveLeasesUntouched(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "active-lease")
	requireCreateRun(t.Context(), t, store, &plan)
	claim := claimNode(t, store)

	recovered, err := store.RecoverExpiredLeases(t.Context())
	require.NoError(t, err)
	assert.Zero(t, recovered)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)
}

func TestStore_RecoverExpiredExhaustedLeasesInBatches(t *testing.T) {
	t.Parallel()

	const (
		batchSize = 100
		runCount  = batchSize + 1
	)

	database, store := newStore(t, true)
	now := time.Now().UTC()

	for index := range runCount {
		plan := validPlan(now, storage.RunID(fmt.Sprintf("batch-recovery-%03d", index)))
		plan.Run.MaxAttempts = 1
		requireCreateRun(t.Context(), t, store, &plan)
		claimNode(t, store)
	}

	_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
		WHERE status = ?`, storage.NodeRunning)
	require.NoError(t, err)

	recovered, err := store.RecoverExpiredLeases(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(batchSize), recovered)
	assertExpiredRunningRunCount(t, database, 1)

	recovered, err = store.RecoverExpiredLeases(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1), recovered)
	assertExpiredRunningRunCount(t, database, 0)

	var failedRuns int

	err = database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM cord_runs WHERE status = ?`, storage.RunFailed).
		Scan(&failedRuns)
	require.NoError(t, err)
	assert.Equal(t, runCount, failedRuns)
}

func assertExpiredRunningRunCount(t *testing.T, database *sql.DB, expected int) {
	t.Helper()

	var count int

	err := database.QueryRowContext(t.Context(), `SELECT COUNT(DISTINCT n.run_id)
		FROM cord_nodes AS n JOIN cord_runs AS r ON r.id = n.run_id
		WHERE n.status = ? AND julianday(n.lease_expires_at) <= julianday('now') AND r.status = ?`,
		storage.NodeRunning,
		storage.RunRunning,
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, expected, count)
}
