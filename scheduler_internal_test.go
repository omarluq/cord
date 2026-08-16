package cord

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func maintenanceTestStep(_ context.Context, value int) (int, error) {
	return value, nil
}

func TestCord_WakeDoesNotRunMaintenance(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "cord.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	store, err := sqlite.New(database)
	require.NoError(t, err)

	schedulerErrors := make(chan error, 1)
	runtime := newCordWithSettings(store, "test-owner", schedulerSettings{
		concurrency:       1,
		pollInterval:      time.Hour,
		leaseTTL:          defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval,
		retry:             defaultRetryPolicy(),
		onSchedulerError:  func(err error) { schedulerErrors <- err },
	})

	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	workflow := runtime.From("maintenance-wake", maintenanceTestStep)
	nodes, err := workflow.graph.compile(workflow.tail)
	require.NoError(t, err)
	plan, err := buildPlan(workflow.graph.name, nodes, workflow.tail, 1, runtime.retry)
	require.NoError(t, err)
	require.NoError(t, store.CreateRun(t.Context(), plan))
	_, err = database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET status = 'retry_wait', available_at = datetime('now', '-1 second')
		WHERE run_id = ?`, plan.Run.ID)
	require.NoError(t, err)

	_, err = database.ExecContext(t.Context(), `CREATE TRIGGER reject_scheduler_maintenance
		BEFORE UPDATE ON cord_nodes BEGIN SELECT RAISE(FAIL, 'maintenance ran'); END`)
	require.NoError(t, err)

	runtime.signalScheduler()
	require.Eventually(t, func() bool { return len(runtime.wake) == 0 }, time.Second, time.Millisecond)

	select {
	case err := <-schedulerErrors:
		t.Fatalf("unexpected scheduler error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}
