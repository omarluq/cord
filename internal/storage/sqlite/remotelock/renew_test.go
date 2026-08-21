package remotelock_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage/sqlite/remotelock"
	"github.com/stretchr/testify/require"
	// Register the SQLite driver used by this test.
	_ "modernc.org/sqlite"
)

func TestRenewRepeats(t *testing.T) {
	t.Parallel()

	connection := openRenewalDatabase(t, "renew.db")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	renewed := make(chan struct{}, 2)
	done := make(chan error, 1)

	go func() {
		done <- remotelock.RenewForTest(ctx, connection, "owner", time.Millisecond, renewalNotifier(renewed))
	}()

	for range 2 {
		select {
		case <-renewed:
		case <-ctx.Done():
			t.Fatal("renewal did not complete twice")
		}
	}

	cancel()
	require.NoError(t, waitForRenewalExit(t, done))
}

func TestRenewCancellationUnblocksNotification(t *testing.T) {
	t.Parallel()

	connection := openRenewalDatabase(t, "blocked-notification.db")

	ctx, cancel := context.WithCancel(t.Context())

	renewed := make(chan struct{}, 1)
	renewed <- struct{}{}

	done := make(chan error, 1)
	entered := make(chan struct{})
	onRenewal := func(ctx context.Context) bool {
		close(entered)

		select {
		case renewed <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		done <- remotelock.RenewForTest(ctx, connection, "owner", time.Millisecond, onRenewal)
	}()

	<-entered
	cancel()
	require.NoError(t, waitForRenewalExit(t, done))
}

func renewalNotifier(renewed chan<- struct{}) func(context.Context) bool {
	return func(ctx context.Context) bool {
		select {
		case renewed <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}
}

func openRenewalDatabase(t *testing.T, name string) *sql.Conn {
	t.Helper()

	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), name))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	connection, err := database.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })

	_, err = connection.ExecContext(t.Context(), `CREATE TABLE cord_migration_lock (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		owner TEXT NOT NULL,
		expires_at INTEGER NOT NULL
	)`)
	require.NoError(t, err)
	_, err = connection.ExecContext(t.Context(),
		"INSERT INTO cord_migration_lock (id, owner, expires_at) VALUES (1, 'owner', 0)")
	require.NoError(t, err)

	return connection
}

func waitForRenewalExit(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("renewal did not stop after cancellation")

		return nil
	}
}
