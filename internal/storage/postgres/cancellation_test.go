package postgres_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	postgresstore "github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCancelRunCapturesTransitionAfterRunLock(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	database := openPostgres(t, dsn)
	require.NoError(t, postgresstore.Migrate(t.Context(), database))

	plan := postgresReadyPlan("cancel-timestamp-order", time.Now().UTC())
	creator, err := postgresstore.New(database)
	require.NoError(t, err)
	require.NoError(t, creator.CreateRun(t.Context(), &plan))

	cancelDatabase := openPostgresPool(t, dsn)
	cancelDatabase.SetMaxOpenConns(1)
	cancelDatabase.SetMaxIdleConns(1)

	applicationName := configureCancellationConnection(t, database, cancelDatabase)

	store, err := postgresstore.New(cancelDatabase)
	require.NoError(t, err)

	locker, err := database.BeginTx(t.Context(), nil)
	require.NoError(t, err)

	committed := false

	t.Cleanup(func() {
		if !committed {
			assert.NoError(t, locker.Rollback())
		}
	})

	var status storage.RunStatus
	require.NoError(t, locker.QueryRowContext(
		t.Context(), `SELECT status FROM cord_runs WHERE id = $1 FOR UPDATE`, plan.Run.ID,
	).Scan(&status))
	require.Equal(t, storage.RunRunning, status)

	type cancellationResult struct {
		err     error
		outcome storage.CancellationOutcome
	}

	result := make(chan cancellationResult, 1)

	go func() {
		outcome, cancelErr := store.CancelRun(t.Context(), plan.Run.ID)
		result <- cancellationResult{outcome: outcome, err: cancelErr}
	}()

	require.Eventually(t, func() bool {
		var waiting bool

		queryErr := database.QueryRowContext(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE application_name = $1 AND wait_event_type = 'Lock'
		)`, applicationName).Scan(&waiting)

		return queryErr == nil && waiting
	}, operationTimeout, 10*time.Millisecond, "cancellation did not wait for the run lock")

	var precedingTransition time.Time
	require.NoError(t, locker.QueryRowContext(t.Context(), `UPDATE cord_runs
		SET status = $1, updated_at = clock_timestamp()
		WHERE id = $2
		RETURNING updated_at`, storage.RunCanceling, plan.Run.ID).Scan(&precedingTransition))
	require.NoError(t, locker.Commit())

	committed = true

	canceled := <-result
	require.NoError(t, canceled.err)
	require.Equal(t, storage.CancellationCanceled, canceled.outcome)

	var updatedAt, completedAt time.Time
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT updated_at, completed_at
		FROM cord_runs WHERE id = $1`, plan.Run.ID).Scan(&updatedAt, &completedAt))
	assert.True(t, updatedAt.After(precedingTransition),
		"cancellation timestamp %s must follow preceding transition %s", updatedAt, precedingTransition)
	assert.Equal(t, updatedAt, completedAt)
}

func configureCancellationConnection(t *testing.T, database, cancelDatabase *sql.DB) string {
	t.Helper()

	var schema string
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema))

	applicationName := "cord_cancel_" + schema
	_, err := cancelDatabase.ExecContext(
		t.Context(), `SELECT set_config('application_name', $1, false)`, applicationName,
	)
	require.NoError(t, err)

	return applicationName
}
