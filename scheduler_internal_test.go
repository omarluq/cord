package cord

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func maintenanceTestStep(_ context.Context, value int) (int, error) {
	return value, nil
}

type heartbeatTestBackend struct {
	storage.Backend
	err      error
	called   chan struct{}
	accepted bool
}

func (backend heartbeatTestBackend) HeartbeatNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	time.Duration,
) (bool, time.Time, error) {
	select {
	case backend.called <- struct{}{}:
	default:
	}

	return backend.accepted, time.Time{}, backend.err
}

func TestCord_HeartbeatFailureCancelsBeforeLeaseExpiry(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		heartbeatErr error
		name         string
		wantReport   bool
	}{
		{name: "lease rejected"},
		{name: "storage errors persist", heartbeatErr: errors.New("heartbeat unavailable"), wantReport: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			called := make(chan struct{}, 1)
			reports := make(chan error, 1)
			backend := heartbeatTestBackend{accepted: false, err: testCase.heartbeatErr, called: called}
			runtime := &Cord{
				store: backend, heartbeatInterval: 50 * time.Millisecond, leaseTTL: 200 * time.Millisecond,
				onSchedulerError: func(err error) { reports <- err }, errorReports: make(chan error, 1),
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan bool, 1)
			claim := &storage.Claim{
				RunID: "", NodeID: "", FunctionKey: "", SignatureHash: "",
				Lease:   storage.Lease{Owner: "", Generation: 0, ExpiresAt: time.Now().Add(time.Hour)},
				Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
			}

			go runtime.heartbeat(ctx, claim, cancel, done)

			<-called

			select {
			case held := <-done:
				require.False(t, held)
			case <-time.After(time.Second):
				t.Fatal("heartbeat did not stop before the test deadline")
			}

			if testCase.wantReport {
				select {
				case report := <-runtime.errorReports:
					require.ErrorContains(t, report, testCase.heartbeatErr.Error())
				case <-time.After(time.Second):
					t.Fatal("heartbeat error was not reported")
				}
			} else {
				require.Empty(t, runtime.errorReports)
			}
		})
	}
}

func TestCord_SchedulerErrorCallbackDoesNotBlockReporting(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "callback.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	store, err := sqlite.New(database)
	require.NoError(t, err)

	callbackStarted := make(chan struct{}, 1)
	callbackRelease := make(chan struct{})
	runtime := newCordWithSettings(store, "callback-owner", schedulerSettings{
		concurrency: 1, pollInterval: time.Hour, leaseTTL: defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval, retry: defaultRetryPolicy(),
		onSchedulerError: func(error) {
			select {
			case callbackStarted <- struct{}{}:
			default:
			}

			<-callbackRelease
		},
	})

	runtime.reportSchedulerError(context.Canceled)
	<-callbackStarted

	reported := make(chan struct{})

	go func() {
		runtime.reportSchedulerError(context.DeadlineExceeded)
		close(reported)
	}()

	<-reported

	shutdownCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, runtime.Shutdown(shutdownCtx), context.Canceled)

	close(callbackRelease)
	require.NoError(t, runtime.Close())
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
