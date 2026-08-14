package storage_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
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
			require.NoError(t, store.CreateRun(t.Context(), &plan))
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
			assert.True(t, expiry.IsZero())

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
	require.NoError(t, store.CreateRun(t.Context(), &plan))
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
	assert.True(t, expiry.IsZero())

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
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)

	accepted, err := store.CancelRun(t.Context(), claim.RunID)
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
	assert.True(t, expiry.IsZero())
}

func TestStore_RecoverExpiredLeasesLeavesActiveLeasesUntouched(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "active-lease")
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)

	recovered, err := store.RecoverExpiredLeases(t.Context())
	require.NoError(t, err)
	assert.Zero(t, recovered)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)
}
