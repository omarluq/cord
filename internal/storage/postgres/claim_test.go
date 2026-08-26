package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipleRuntimesClaimEachRunOnce(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	options := cord.Options{Concurrency: 16, PollInterval: time.Millisecond}
	first, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)
	second, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, first.Close())
		assert.NoError(t, second.Close())
	})

	firstFlow := first.From("postgres-shared-claims", postgresAddOne)
	secondFlow := second.From("postgres-shared-claims", postgresAddOne)

	const runs = 100

	runErrors := make(chan error, runs)

	var group sync.WaitGroup
	for index := range runs {
		group.Go(func() {
			flow := firstFlow
			if index%2 != 0 {
				flow = secondFlow
			}

			result, runErr := flow.Run(t.Context(), index)
			if runErr == nil && result != index+1 {
				runErr = fmt.Errorf("result = %d, want %d", result, index+1)
			}

			runErrors <- runErr
		})
	}

	group.Wait()
	close(runErrors)

	for runErr := range runErrors {
		require.NoError(t, runErr)
	}

	var duplicateAttempts int

	const duplicateQuery = `SELECT count(*) FROM cord_nodes n JOIN cord_runs r ON r.id=n.run_id
		WHERE r.workflow_name=$1 AND n.attempt <> 1`
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		duplicateQuery,
		"postgres-shared-claims",
	).Scan(&duplicateAttempts))
	assert.Zero(t, duplicateAttempts)
}

func TestConcurrentClaimersAcrossPoolsClaimEachNodeOnce(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	database := openPostgres(t, dsn)
	require.NoError(t, postgres.Migrate(t.Context(), database))

	const (
		claimers = 100
		pools    = 4
	)

	stores := make([]*postgres.Store, pools)
	for index := range stores {
		store, err := postgres.New(openPostgresPool(t, dsn))
		require.NoError(t, err)

		stores[index] = store
	}

	for index := range claimers {
		plan := postgresReadyPlan(storage.RunID(fmt.Sprintf("concurrent-claim-%03d", index)), time.Now().UTC())
		createErr := stores[0].CreateRun(t.Context(), &plan)
		require.NoError(t, createErr)
	}

	start := make(chan struct{})
	claims := make(chan *storage.Claim, claimers)
	errs := make(chan error, claimers)

	var group sync.WaitGroup
	for index := range claimers {
		group.Go(func() {
			<-start

			claim, claimed, err := stores[index%pools].ClaimReadyNodeForFunctions(
				t.Context(), fmt.Sprintf("claimer-%03d", index), time.Minute, postgresRegistrations())
			if err == nil && !claimed {
				err = fmt.Errorf("claimer %d did not claim a node", index)
			}

			if err != nil {
				errs <- err

				return
			}

			claims <- claim
		})
	}

	close(start)
	group.Wait()
	close(claims)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	unique := make(map[string]struct{}, claimers)

	for claim := range claims {
		key := string(claim.RunID) + "/" + string(claim.NodeID)
		_, duplicate := unique[key]
		assert.False(t, duplicate, "duplicate claim %s", key)
		unique[key] = struct{}{}
	}

	assert.Len(t, unique, claimers)
}

func TestClaimSkipsLockedFirstCandidate(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	database := openPostgres(t, dsn)
	require.NoError(t, postgres.Migrate(t.Context(), database))
	store, err := postgres.New(openPostgresPool(t, dsn))
	require.NoError(t, err)

	first := postgresReadyPlan("locked-first", time.Now().UTC().Add(-time.Second))
	second := postgresReadyPlan("unlocked-second", time.Now().UTC())

	err = store.CreateRun(t.Context(), &first)
	require.NoError(t, err)
	err = store.CreateRun(t.Context(), &second)
	require.NoError(t, err)

	transaction, err := database.BeginTx(t.Context(), nil)

	require.NoError(t, err)
	defer func() { assert.NoError(t, transaction.Rollback()) }()

	var locked storage.RunID

	err = transaction.QueryRowContext(t.Context(), `SELECT run_id FROM cord_nodes
		WHERE status='ready' ORDER BY available_at, run_id, node_id LIMIT 1 FOR UPDATE`).Scan(&locked)
	require.NoError(t, err)
	require.Equal(t, first.Run.ID, locked)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	claim, claimed, err := store.ClaimReadyNodeForFunctions(
		ctx, "skip-locked-worker", time.Minute, postgresRegistrations(),
	)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotNil(t, claim)
	assert.Equal(t, second.Run.ID, claim.RunID)
}
