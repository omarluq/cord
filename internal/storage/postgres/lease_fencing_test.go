package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staleLeaseOperation struct {
	run  func(context.Context, *postgres.Store, *storage.Claim) (bool, error)
	name string
}

// TestStaleLeasesRejectTransitionsWithoutMutation verifies PostgreSQL lease fencing.
func TestStaleLeasesRejectTransitionsWithoutMutation(t *testing.T) {
	t.Parallel()

	operations := []staleLeaseOperation{
		{run: func(ctx context.Context, store *postgres.Store, claim *storage.Claim) (bool, error) {
			return store.CompleteNode(ctx, claim.RunID, claim.NodeID, claim.Lease, []byte("complete"))
		}, name: "complete"},
		{run: func(ctx context.Context, store *postgres.Store, claim *storage.Claim) (bool, error) {
			return store.RetryNode(ctx, claim.RunID, claim.NodeID, claim.Lease, []byte("retry"), time.Minute)
		}, name: "retry"},
		{run: func(ctx context.Context, store *postgres.Store, claim *storage.Claim) (bool, error) {
			return store.FailNode(
				ctx, claim.RunID, claim.NodeID, claim.Lease, []byte("fail"),
				storage.ReasonFailureAttemptsExhausted,
			)
		}, name: "fail"},
		{run: func(ctx context.Context, store *postgres.Store, claim *storage.Claim) (bool, error) {
			accepted, _, err := store.HeartbeatNode(ctx, claim.RunID, claim.NodeID, claim.Lease, time.Minute)
			if err != nil {
				return false, fmt.Errorf("heartbeat stale lease: %w", err)
			}

			return accepted, nil
		}, name: "heartbeat"},
	}
	fences := []struct {
		mutate func(context.Context, *sql.DB, *storage.Claim) error
		name   string
	}{
		{mutate: func(_ context.Context, _ *sql.DB, claim *storage.Claim) error {
			claim.Lease.Owner = "stale-owner"

			return nil
		}, name: "owner"},
		{mutate: func(_ context.Context, _ *sql.DB, claim *storage.Claim) error {
			claim.Lease.Generation--

			return nil
		}, name: "generation"},
		{mutate: func(ctx context.Context, database *sql.DB, claim *storage.Claim) error {
			const query = `UPDATE cord_nodes
				SET lease_expires_at=TIMESTAMPTZ '2000-01-01 00:00:00+00'
				WHERE run_id=$1 AND node_id=$2`
			if _, err := database.ExecContext(ctx, query, claim.RunID, claim.NodeID); err != nil {
				return fmt.Errorf("expire lease: %w", err)
			}

			return nil
		}, name: "expired"},
	}

	database := openPostgres(t, startPostgres(t))
	require.NoError(t, postgres.Migrate(t.Context(), database))
	store, err := postgres.New(database)
	require.NoError(t, err)

	for _, operation := range operations {
		for _, fence := range fences {
			runID := storage.RunID("fence-" + operation.name + "-" + fence.name)
			plan := postgresReadyPlan(runID, time.Now().UTC())
			err = store.CreateRun(t.Context(), &plan)
			require.NoError(t, err, operation.name+"/stale_"+fence.name)
			claim, claimed, claimErr := store.ClaimReadyNodeForFunctions(
				t.Context(), "current-owner", time.Minute, postgresRegistrations(),
			)
			require.NoError(t, claimErr, operation.name+"/stale_"+fence.name)
			require.True(t, claimed, operation.name+"/stale_"+fence.name)
			require.Equal(t, runID, claim.RunID, operation.name+"/stale_"+fence.name)
			require.NoError(t, fence.mutate(t.Context(), database, claim), operation.name+"/stale_"+fence.name)
			before := postgresDurableState(t, database, runID, claim.NodeID)
			accepted, operationErr := operation.run(t.Context(), store, claim)
			require.NoError(t, operationErr, operation.name+"/stale_"+fence.name)
			assert.False(t, accepted, operation.name+"/stale_"+fence.name)
			assert.Equal(
				t, before, postgresDurableState(t, database, runID, claim.NodeID),
				operation.name+"/stale_"+fence.name,
			)
		}
	}
}
