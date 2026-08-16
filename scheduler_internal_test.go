package cord

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestCord_WakeDoesNotRunMaintenance(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "cord.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	require.NoError(t, storage.Migrate(t.Context(), database))
	store, err := storage.NewStore(database)
	require.NoError(t, err)

	schedulerErrors := make(chan error, 1)
	runtime := newCordWithSettings(store, "test-owner", schedulerSettings{
		concurrency:       1,
		pollInterval:      time.Hour,
		leaseTTL:          defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval,
		onSchedulerError:  func(err error) { schedulerErrors <- err },
	})

	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	_, err = database.ExecContext(t.Context(), `CREATE TRIGGER reject_scheduler_maintenance
		BEFORE UPDATE ON cord_nodes BEGIN SELECT RAISE(FAIL, 'maintenance ran'); END`)
	require.NoError(t, err)

	runtime.signalScheduler()

	select {
	case <-runtime.wake:
		t.Fatal("scheduler did not consume wake signal")
	case <-time.After(time.Second):
	}

	select {
	case err := <-schedulerErrors:
		t.Fatalf("unexpected scheduler error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}
