package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestStore_ClaimReadyNodeValidatesLease(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)

	tests := []struct {
		name  string
		owner string
		ttl   time.Duration
	}{
		{name: "empty owner", owner: "", ttl: time.Minute},
		{name: "zero TTL", owner: "worker", ttl: 0},
		{name: "negative TTL", owner: "worker", ttl: -time.Second},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			claim, claimed, err := store.ClaimReadyNode(t.Context(), testCase.owner, testCase.ttl)
			require.Error(t, err)
			assert.False(t, claimed)
			assert.Nil(t, claim)
		})
	}
}

func TestStore_ClaimReadyNodeUsesDatabaseEligibilityAndCAS(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	now := time.Now().UTC()
	plan := validPlan(now, "claim")
	plan.Nodes[0].AvailableAt = now.Add(time.Hour)
	requireCreateRun(t.Context(), t, store, &plan)

	claim, claimed, err := store.ClaimReadyNode(t.Context(), "worker-a", time.Minute)
	require.NoError(t, err)
	assert.False(t, claimed)
	assert.Nil(t, claim)

	_, err = database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
		WHERE run_id = ? AND node_id = ?`, plan.Run.ID, plan.Nodes[0].ID)
	require.NoError(t, err)

	claim, claimed, err = store.ClaimReadyNode(t.Context(), "worker-a", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotNil(t, claim)
	assert.Equal(t, plan.Run.ID, claim.RunID)
	assert.Equal(t, plan.Nodes[0].ID, claim.NodeID)
	assert.Equal(t, "worker-a", claim.Lease.Owner)
	assert.Equal(t, int64(1), claim.Lease.Generation)
	assert.Equal(t, 1, claim.Attempt)
	assert.Equal(t, plan.Run.MaxAttempts, claim.MaxAttempts)
	assert.Equal(t, plan.Run.RetryBaseDelay, claim.RetryBaseDelay)
	assert.Equal(t, plan.Run.RetryMaxDelay, claim.RetryMaxDelay)
	assert.Equal(t, plan.Run.RetryPolicyVersion, claim.RetryPolicyVersion)
	assert.WithinDuration(t, time.Now().UTC().Add(time.Minute), claim.Lease.ExpiresAt, 2*time.Second)

	second, won, err := store.ClaimReadyNode(t.Context(), "worker-b", time.Minute)
	require.NoError(t, err)
	assert.False(t, won)
	assert.Nil(t, second)
	assertNodeState(t, database, plan.Run.ID, plan.Nodes[0].ID, storage.NodeRunning, 0)
}

func TestStore_HeartbeatExtendsLeaseAndRejectsLoss(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "heartbeat")
	requireCreateRun(t.Context(), t, store, &plan)
	claim := claimNode(t, store)

	accepted, expiry, err := store.HeartbeatNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, 2*time.Minute,
	)
	require.NoError(t, err)
	assert.True(t, accepted)
	assert.Positive(t, expiry)

	_, err = database.ExecContext(t.Context(), `UPDATE cord_nodes SET lease_generation = lease_generation + 1
		WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID)
	require.NoError(t, err)
	accepted, _, err = store.HeartbeatNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, 2*time.Minute,
	)
	require.NoError(t, err)
	assert.False(t, accepted)
}

func TestStore_ExpiredRecoveryIncrementsFenceAndAttempt(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "recovery")
	requireCreateRun(t.Context(), t, store, &plan)
	first := claimNode(t, store)

	_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
		WHERE run_id = ? AND node_id = ?`,
		first.RunID, first.NodeID)
	require.NoError(t, err)
	recovered, err := store.RecoverExpiredLeases(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1), recovered)

	second := claimNode(t, store)
	assert.Equal(t, 2, second.Attempt)
	assert.Greater(t, second.Lease.Generation, first.Lease.Generation)

	accepted, err := store.CompleteNode(
		t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"stale"`),
	)
	require.NoError(t, err)
	assert.False(t, accepted)
	accepted, err = store.CompleteNode(
		t.Context(), second.RunID, second.NodeID, second.Lease, []byte(`"current"`),
	)
	require.NoError(t, err)
	assert.True(t, accepted)
}

func TestStore_ClaimCommitSurvivesProcessBoundary(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "crash.db")
	database := openStoreDatabase(t, path)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	store, err := sqlite.New(database)
	require.NoError(t, err)

	plan := validPlan(time.Now().UTC(), "claim-crash")
	requireCreateRun(t.Context(), t, store, &plan)

	claim, claimed, err := store.ClaimReadyNode(t.Context(), "departed-worker", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, database.Close())

	reopened := openStoreDatabase(t, path)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	assertNodeState(t, reopened, plan.Run.ID, claim.NodeID, storage.NodeRunning, 0)
}
