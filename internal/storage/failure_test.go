package storage_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_RetryNodeRejectsNegativeDelayWithoutMutation(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "negative-retry-delay")
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)

	accepted, err := store.RetryNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("must not persist"), -time.Second,
	)
	require.ErrorContains(t, err, "delay must not be negative")
	assert.False(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)

	var failure []byte

	err = database.QueryRowContext(t.Context(), `SELECT error_payload FROM cord_nodes
		WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID).Scan(&failure)
	require.NoError(t, err)
	assert.Nil(t, failure)
}

func TestStore_RetryNodeRejectsInvalidFencesWithoutMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*storage.Lease)
		name   string
		expire bool
	}{
		{name: "wrong owner", mutate: func(lease *storage.Lease) { lease.Owner = "other-worker" }, expire: false},
		{name: "stale generation", mutate: func(lease *storage.Lease) { lease.Generation-- }, expire: false},
		{name: "expired lease", mutate: nil, expire: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("retry-rejection-"+testCase.name))
			require.NoError(t, store.CreateRun(t.Context(), &plan))
			claim := claimNode(t, store)

			if testCase.mutate != nil {
				testCase.mutate(&claim.Lease)
			}

			if testCase.expire {
				_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
					SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
					WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID)
				require.NoError(t, err)
			}

			accepted, err := store.RetryNode(
				t.Context(),
				claim.RunID,
				claim.NodeID,
				claim.Lease,
				[]byte("must not persist"),
				time.Minute,
			)
			require.NoError(t, err)
			assert.False(t, accepted)
			assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)

			var failure []byte

			err = database.QueryRowContext(t.Context(), `SELECT error_payload FROM cord_nodes
				WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID).Scan(&failure)
			require.NoError(t, err)
			assert.Nil(t, failure)
		})
	}
}

func TestStore_RetryNodePersistsFailureAndClearsLease(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "retry-fence")
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)
	failure := []byte(`{"message":"retry"}`)
	accepted, err := store.RetryNode(
		t.Context(),
		claim.RunID,
		claim.NodeID,
		claim.Lease,
		failure,
		time.Minute,
	)
	require.NoError(t, err)
	require.True(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRetryWait, 0)

	var (
		actualFailure []byte
		leaseOwner    *string
		leaseExpiry   *string
	)

	err = database.QueryRowContext(t.Context(), `SELECT error_payload, lease_owner, lease_expires_at
		FROM cord_nodes WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID).
		Scan(&actualFailure, &leaseOwner, &leaseExpiry)
	require.NoError(t, err)
	assert.Equal(t, failure, actualFailure)
	assert.Nil(t, leaseOwner)
	assert.Nil(t, leaseExpiry)
}

func TestStore_FailNodeRollsBackWhenRunFailureFails(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "failure-rollback")
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)
	trigger := `CREATE TRIGGER reject_failed_run BEFORE UPDATE OF status ON cord_runs
		WHEN NEW.status = 'failed' BEGIN SELECT RAISE(ABORT, 'run failure boundary'); END`

	_, err := database.ExecContext(t.Context(), trigger)
	require.NoError(t, err)

	accepted, err := store.FailNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("rollback"))
	require.ErrorContains(t, err, "run failure boundary")
	assert.False(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)
	assertNodeState(t, database, claim.RunID, "publish", storage.NodePending, 1)
	assertRunState(t, database, claim.RunID, storage.RunRunning, nil)
}
