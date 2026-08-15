package storage_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// Register the SQLite driver used by newStore.
	_ "modernc.org/sqlite"
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
	require.NoError(t, store.CreateRun(t.Context(), &plan))

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
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)

	accepted, expiry, err := store.HeartbeatNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, 2*time.Minute,
	)
	require.NoError(t, err)
	assert.True(t, accepted)
	assert.True(t, expiry.After(claim.Lease.ExpiresAt))

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
	require.NoError(t, store.CreateRun(t.Context(), &plan))
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
	require.NoError(t, storage.Migrate(t.Context(), database))
	store, err := storage.NewStore(database)
	require.NoError(t, err)

	plan := validPlan(time.Now().UTC(), "claim-crash")
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	claim, claimed, err := store.ClaimReadyNode(t.Context(), "departed-worker", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, database.Close())

	reopened := openStoreDatabase(t, path)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	assertNodeState(t, reopened, plan.Run.ID, claim.NodeID, storage.NodeRunning, 0)
}

func TestStore_CompleteNodeReleasesDependenciesAndCompletesTerminalRun(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "complete")
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	first := claimNode(t, store)
	accepted, err := store.CompleteNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"object"`))
	require.NoError(t, err)
	require.True(t, accepted)
	assertNodeState(t, database, first.RunID, first.NodeID, storage.NodeCompleted, 0)
	assertNodeState(t, database, first.RunID, "publish", storage.NodeReady, 0)
	assertRunState(t, database, first.RunID, storage.RunRunning, nil)

	terminal := claimNode(t, store)
	accepted, err = store.CompleteNode(
		t.Context(), terminal.RunID, terminal.NodeID, terminal.Lease, []byte(`"released"`),
	)
	require.NoError(t, err)
	require.True(t, accepted)
	assertRunState(t, database, terminal.RunID, storage.RunCompleted, []byte(`"released"`))

	accepted, err = store.CompleteNode(
		t.Context(), terminal.RunID, terminal.NodeID, terminal.Lease, []byte(`"overwrite"`),
	)
	require.NoError(t, err)
	assert.False(t, accepted)
	assertRunState(t, database, terminal.RunID, storage.RunCompleted, []byte(`"released"`))
}

func TestStore_CompleteNodeQueuesReleasedChildrenBehindWaitingWork(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	now := time.Now().UTC()
	active := validPlan(now.Add(-2*time.Minute), "active-run")
	waiting := validPlan(now.Add(-time.Minute), "waiting-run")

	require.NoError(t, store.CreateRun(t.Context(), &active))
	require.NoError(t, store.CreateRun(t.Context(), &waiting))

	first := claimNode(t, store)
	require.Equal(t, active.Run.ID, first.RunID)

	accepted, err := store.CompleteNode(
		t.Context(),
		first.RunID,
		first.NodeID,
		first.Lease,
		[]byte(`"compiled"`),
	)
	require.NoError(t, err)
	require.True(t, accepted)

	next := claimNode(t, store)
	assert.Equal(t, waiting.Run.ID, next.RunID)
	assert.Equal(t, compileNode, next.NodeID)

	var releasedLater bool
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		`SELECT julianday(released.available_at) > julianday(waiting.available_at)
		FROM cord_nodes AS released, cord_nodes AS waiting
		WHERE released.run_id = ? AND released.node_id = ?
			AND waiting.run_id = ? AND waiting.node_id = ?`,
		active.Run.ID,
		active.Run.TerminalNodeID,
		waiting.Run.ID,
		compileNode,
	).Scan(&releasedLater))
	assert.True(t, releasedLater)
}

func TestStore_CompleteNodeDoesNotStarveReadyBranch(t *testing.T) {
	t.Parallel()

	const joinNode storage.NodeID = "join"

	_, store := newStore(t, true)
	now := time.Now().UTC()
	plan := validPlan(now.Add(-2*time.Minute), "branch-fairness")
	plan.Nodes = append(plan.Nodes,
		newNode(
			plan.Run.ID,
			"waiting-branch",
			"example.com/workflow.Waiting",
			"waiting-signature",
			storage.NodeReady,
			now.Add(-time.Minute),
			0,
		),
		newNode(
			plan.Run.ID,
			joinNode,
			"example.com/workflow.Join",
			"join-signature",
			storage.NodePending,
			now,
			2,
		),
	)
	plan.Edges = append(plan.Edges,
		storage.Edge{RunID: plan.Run.ID, Parent: terminalNode, Child: joinNode, ParentOrder: 0},
		storage.Edge{RunID: plan.Run.ID, Parent: "waiting-branch", Child: joinNode, ParentOrder: 1},
	)
	plan.Run.TerminalNodeID = joinNode
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	first := claimNode(t, store)
	require.Equal(t, compileNode, first.NodeID)
	accepted, err := store.CompleteNode(
		t.Context(),
		first.RunID,
		first.NodeID,
		first.Lease,
		[]byte(`"compiled"`),
	)
	require.NoError(t, err)
	require.True(t, accepted)

	next := claimNode(t, store)
	assert.Equal(t, storage.NodeID("waiting-branch"), next.NodeID)
}

func TestStore_CompleteNodeRejectsStaleAndExpiredLeases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*storage.Lease)
		name   string
	}{
		{name: "owner", mutate: func(lease *storage.Lease) { lease.Owner = "stale" }},
		{name: "generation", mutate: func(lease *storage.Lease) { lease.Generation-- }},
		{name: "expired in database", mutate: nil},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("fence-"+testCase.name))
			require.NoError(t, store.CreateRun(t.Context(), &plan))
			claim := claimNode(t, store)

			if testCase.mutate == nil {
				_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
					SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
					WHERE run_id = ? AND node_id = ?`, claim.RunID, claim.NodeID)
				require.NoError(t, err)
			} else {
				testCase.mutate(&claim.Lease)
			}

			accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("bad"))
			require.NoError(t, err)
			assert.False(t, accepted)
			assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)
			assertNodeState(t, database, claim.RunID, "publish", storage.NodePending, 1)
		})
	}
}

func TestStore_CompleteNodeRollsBackAtDependencyReleaseBoundary(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "completion-rollback")
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)

	_, err := database.ExecContext(t.Context(), `CREATE TRIGGER reject_ready BEFORE UPDATE OF status ON cord_nodes
		WHEN NEW.status = 'ready' BEGIN SELECT RAISE(ABORT, 'crash boundary'); END`)
	require.NoError(t, err)

	accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("must-rollback"))
	require.ErrorContains(t, err, "crash boundary")
	assert.False(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRunning, 0)
	assertNodeState(t, database, claim.RunID, "publish", storage.NodePending, 1)
	assertRunState(t, database, claim.RunID, storage.RunRunning, nil)
}

func TestStore_FailNodeIsFencedAndAtomic(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "failure")
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)
	stale := claim.Lease
	stale.Generation--

	accepted, err := store.FailNode(t.Context(), claim.RunID, claim.NodeID, stale, []byte("stale"))
	require.NoError(t, err)
	assert.False(t, accepted)
	assertRunState(t, database, claim.RunID, storage.RunRunning, nil)

	accepted, err = store.FailNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("permanent"))
	require.NoError(t, err)
	require.True(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeFailed, 0)
	assertNodeState(t, database, claim.RunID, "publish", storage.NodeCanceled, 1)
	assertRunFailure(t, database, claim.RunID, []byte("permanent"))
}

func TestStore_CancelRunFencesRunningWorkAndPreservesCompletedNodes(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "cancel")
	require.NoError(t, store.CreateRun(t.Context(), &plan))
	claim := claimNode(t, store)

	accepted, err := store.CancelRun(t.Context(), claim.RunID)
	require.NoError(t, err)
	require.True(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeCanceled, 0)
	assertNodeState(t, database, claim.RunID, "publish", storage.NodeCanceled, 1)
	assertRunState(t, database, claim.RunID, storage.RunCanceled, nil)

	completed, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte("late"))
	require.NoError(t, err)
	assert.False(t, completed)
	accepted, err = store.CancelRun(t.Context(), claim.RunID)
	require.NoError(t, err)
	assert.False(t, accepted)
}

func claimNode(t *testing.T, store *storage.Store) *storage.Claim {
	t.Helper()

	claim, claimed, err := store.ClaimReadyNode(t.Context(), "worker", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotNil(t, claim)

	return claim
}

func assertNodeState(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	nodeID storage.NodeID,
	status storage.NodeStatus,
	remainingDeps int,
) {
	t.Helper()

	var (
		actualStatus    storage.NodeStatus
		actualRemaining int
	)

	err := database.QueryRowContext(t.Context(), `SELECT status, remaining_deps FROM cord_nodes
		WHERE run_id = ? AND node_id = ?`, runID, nodeID).Scan(&actualStatus, &actualRemaining)
	require.NoError(t, err)
	assert.Equal(t, status, actualStatus)
	assert.Equal(t, remainingDeps, actualRemaining)
}

func assertRunState(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	status storage.RunStatus,
	output []byte,
) {
	t.Helper()

	var (
		actualStatus storage.RunStatus
		actualOutput []byte
	)

	err := database.QueryRowContext(t.Context(), `SELECT status, output_payload FROM cord_runs WHERE id = ?`, runID).
		Scan(&actualStatus, &actualOutput)
	require.NoError(t, err)
	assert.Equal(t, status, actualStatus)
	assert.Equal(t, output, actualOutput)
}

func assertRunFailure(t *testing.T, database *sql.DB, runID storage.RunID, failure []byte) {
	t.Helper()

	var (
		status storage.RunStatus
		actual []byte
	)

	err := database.QueryRowContext(t.Context(), `SELECT status, error_payload FROM cord_runs WHERE id = ?`, runID).
		Scan(&status, &actual)
	require.NoError(t, err)
	assert.Equal(t, storage.RunFailed, status)
	assert.Equal(t, failure, actual)
}

func openStoreDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)

	return database
}
