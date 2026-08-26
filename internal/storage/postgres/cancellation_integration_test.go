package postgres_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCancelRunOutcomesAndFencing(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	require.NoError(t, postgres.Migrate(t.Context(), database))
	store, err := postgres.New(database)
	require.NoError(t, err)

	plan := postgresReadyPlan("cancel-groundwork", time.Now().UTC())
	err = store.CreateRun(t.Context(), &plan)
	require.NoError(t, err)
	claim := claimPostgresNode(t, store, "worker", "postgres.test", "signature")

	outcome, err := store.CancelRun(t.Context(), claim.RunID)
	require.NoError(t, err)
	require.Equal(t, storage.CancellationCanceled, outcome)

	result, err := store.GetRunResult(t.Context(), claim.RunID)
	require.NoError(t, err)
	assert.Equal(t, storage.RunCanceled, result.Status)

	accepted, err := store.CompleteNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"late"`),
	)
	require.NoError(t, err)
	assert.False(t, accepted)

	outcome, err = store.CancelRun(t.Context(), claim.RunID)
	require.NoError(t, err)
	assert.Equal(t, storage.CancellationAlreadyCanceled, outcome)

	outcome, err = store.CancelRun(t.Context(), "missing-run")
	require.NoError(t, err)
	assert.Equal(t, storage.CancellationNotFound, outcome)
}
