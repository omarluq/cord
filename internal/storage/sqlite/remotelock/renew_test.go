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

	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "renew.db"))
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

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	renewed := make(chan struct{}, 2)
	done := make(chan error, 1)

	go func() {
		done <- remotelock.RenewForTest(ctx, connection, "owner", time.Millisecond, renewed)
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

	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "blocked-notification.db"))
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

	ctx, cancel := context.WithCancel(t.Context())

	renewed := make(chan struct{}, 1)
	renewed <- struct{}{}

	done := make(chan error, 1)

	go func() {
		done <- remotelock.RenewForTest(ctx, connection, "owner", time.Millisecond, renewed)
	}()

	require.Eventually(t, func() bool {
		var expiresAt int64

		err := database.QueryRowContext(t.Context(),
			"SELECT expires_at FROM cord_migration_lock WHERE id = 1").Scan(&expiresAt)

		return err == nil && expiresAt > 0
	}, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, waitForRenewalExit(t, done))
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
