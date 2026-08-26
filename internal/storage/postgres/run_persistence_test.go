package postgres_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateRunPersistsNodeAvailabilitySeparatelyFromStateChange(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	require.NoError(t, postgres.Migrate(t.Context(), database))
	store, err := postgres.New(database)
	require.NoError(t, err)

	availableAt := time.Date(2040, time.January, 2, 3, 4, 5, 6000, time.UTC)
	plan := postgresReadyPlan("persist-node-times", availableAt)
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	var persistedAvailableAt, stateChangedAt, runCreatedAt time.Time

	err = database.QueryRowContext(t.Context(), `SELECT
		 n.available_at, n.state_changed_at, r.created_at
		 FROM cord_nodes n JOIN cord_runs r ON r.id = n.run_id
		 WHERE n.run_id = $1 AND n.node_id = $2`, plan.Run.ID, postgresTestNode).Scan(
		&persistedAvailableAt,
		&stateChangedAt,
		&runCreatedAt,
	)
	require.NoError(t, err)
	assert.True(t, persistedAvailableAt.Equal(availableAt))
	assert.True(t, stateChangedAt.Equal(runCreatedAt))
	assert.False(t, stateChangedAt.Equal(availableAt))
}
