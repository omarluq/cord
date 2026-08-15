package storage_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_ClaimReadyNodeForFunctionsValidatesLease(t *testing.T) {
	t.Parallel()

	const worker = "registered-worker"

	tests := []struct {
		name  string
		owner string
		ttl   time.Duration
	}{
		{name: "empty owner", owner: "", ttl: time.Minute},
		{name: "zero TTL", owner: worker, ttl: 0},
		{name: "negative TTL", owner: worker, ttl: -time.Second},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, store := newStore(t, true)
			plan := validPlan(
				time.Now().UTC(),
				storage.RunID("registered-claim-validation-"+testCase.name),
			)
			require.NoError(t, store.CreateRun(t.Context(), &plan))
			registered := []byte(`{"` + plan.Nodes[0].FunctionKey + `":"` + plan.Nodes[0].SignatureHash + `"}`)

			claim, claimed, err := store.ClaimReadyNodeForFunctions(
				t.Context(),
				testCase.owner,
				testCase.ttl,
				registered,
			)
			require.Error(t, err)
			assert.False(t, claimed)
			assert.Nil(t, claim)
		})
	}
}

// TestStore_ClaimReadyNodeRejectsExhaustedAttempts verifies exhausted nodes cannot be claimed.
func TestStore_ClaimReadyNodeRejectsExhaustedAttempts(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "exhausted-run")
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	result, err := database.ExecContext(
		t.Context(),
		"UPDATE cord_nodes SET attempt = ? WHERE run_id = ? AND node_id = ?",
		plan.Run.MaxAttempts,
		plan.Run.ID,
		plan.Nodes[0].ID,
	)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)

	claim, claimed, err := store.ClaimReadyNode(t.Context(), "worker", time.Minute)
	require.NoError(t, err)
	assert.False(t, claimed)
	assert.Nil(t, claim)
}

func TestStore_ClaimReadyNodeUsesDeterministicEligibilityOrder(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	now := time.Now().UTC()
	later := validPlan(now.Add(time.Second), "later-run")
	earlier := validPlan(now, "earlier-run")

	require.NoError(t, store.CreateRun(t.Context(), &later))
	require.NoError(t, store.CreateRun(t.Context(), &earlier))

	claim := claimNode(t, store)
	assert.Equal(t, earlier.Run.ID, claim.RunID)
	assert.Equal(t, earlier.Nodes[0].ID, claim.NodeID)
}
