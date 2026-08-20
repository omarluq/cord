package remotelock_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage/sqlite/remotelock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestLockerAcquiresAndReleases(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	locker := remotelock.New(database, nil)

	require.NoError(t, locker.SessionLock(t.Context(), connection))
	assert.Equal(t, 1, lockRows(t, database))

	require.NoError(t, locker.SessionUnlock(t.Context(), connection))
	assert.Equal(t, 0, lockRows(t, database))
}

func TestLockerContentionHonorsCancellation(t *testing.T) {
	t.Parallel()

	database, firstConnection := openDatabase(t)
	secondConnection, err := database.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, secondConnection.Close()) })

	first := remotelock.New(database, nil)
	require.NoError(t, first.SessionLock(t.Context(), firstConnection))
	t.Cleanup(func() { require.NoError(t, first.SessionUnlock(context.Background(), firstConnection)) })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = remotelock.New(database, nil).SessionLock(ctx, secondConnection)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLockerTakesOverAbandonedLease(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	createLockTable(t, database)
	_, err := database.ExecContext(t.Context(),
		"INSERT INTO cord_migration_lock (id, owner, expires_at) VALUES (1, 'abandoned', 0)")
	require.NoError(t, err)

	locker := remotelock.New(database, nil)
	require.NoError(t, locker.SessionLock(t.Context(), connection))

	var owner string
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT owner FROM cord_migration_lock WHERE id = 1").Scan(&owner))
	assert.NotEqual(t, "abandoned", owner)
	require.NoError(t, locker.SessionUnlock(t.Context(), connection))
}

func TestLockerRenewsLease(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	createLockTable(t, database)
	_, err := database.ExecContext(t.Context(), "CREATE TABLE renewal_events (renewed_at INTEGER NOT NULL)")
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), `CREATE TRIGGER observe_renewal
		AFTER UPDATE ON cord_migration_lock
		BEGIN INSERT INTO renewal_events VALUES (unixepoch()); END`)
	require.NoError(t, err)

	locker := remotelock.New(database, nil)
	require.NoError(t, locker.SessionLock(t.Context(), connection))

	require.Eventually(t, func() bool {
		var count int

		queryErr := database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM renewal_events").Scan(&count)

		return queryErr == nil && count > 0
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, locker.SessionUnlock(t.Context(), connection))
}

func TestLockerReportsRenewalOwnershipLoss(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	createLockTable(t, database)
	_, err := database.ExecContext(t.Context(), `CREATE TRIGGER reject_renewal
		BEFORE UPDATE ON cord_migration_lock
		BEGIN SELECT RAISE(IGNORE); END`)
	require.NoError(t, err)

	migrationCtx, cancelMigration := context.WithCancelCause(t.Context())
	locker := remotelock.New(database, cancelMigration)
	require.NoError(t, locker.SessionLock(t.Context(), connection))

	select {
	case <-migrationCtx.Done():
		require.EqualError(t, context.Cause(migrationCtx), "remote migration lock ownership lost")
	case <-time.After(time.Second):
		t.Fatal("renewal did not report ownership loss")
	}

	err = locker.SessionUnlock(t.Context(), connection)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remote migration lock ownership lost")
}

func TestLockerReportsOwnershipLossOnUnlock(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	locker := remotelock.New(database, nil)
	require.NoError(t, locker.SessionLock(t.Context(), connection))
	_, err := database.ExecContext(t.Context(), "UPDATE cord_migration_lock SET owner = 'replacement' WHERE id = 1")
	require.NoError(t, err)

	err = locker.SessionUnlock(t.Context(), connection)
	require.EqualError(t, err, "remote migration lock ownership lost")
}

func TestLockerUnlockWithoutLockIsNoOp(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	require.NoError(t, connection.Close())
	require.NoError(t, remotelock.New(database, nil).SessionUnlock(t.Context(), connection))
}

func TestLockerReturnsClosedConnectionErrors(t *testing.T) {
	t.Parallel()

	t.Run("acquire", func(t *testing.T) {
		t.Parallel()

		database, connection := openDatabase(t)
		require.NoError(t, connection.Close())

		err := remotelock.New(database, nil).SessionLock(t.Context(), connection)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create remote migration lock table")
	})

	t.Run("release", func(t *testing.T) {
		t.Parallel()

		database, connection := openDatabase(t)
		locker := remotelock.New(database, nil)
		require.NoError(t, locker.SessionLock(t.Context(), connection))
		require.NoError(t, connection.Close())

		err := locker.SessionUnlock(t.Context(), connection)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release remote migration lock")
	})
}

func TestLockerReturnsClosedDatabaseRenewalError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "remote-lock.db")
	lockerDatabase := openPath(t, path)
	connectionDatabase := openPath(t, path)
	connection, err := connectionDatabase.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		closeErr := connection.Close()
		if closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			require.NoError(t, closeErr)
		}
	})
	require.NoError(t, lockerDatabase.Close())

	migrationCtx, cancelMigration := context.WithCancelCause(t.Context())
	locker := remotelock.New(lockerDatabase, cancelMigration)
	require.NoError(t, locker.SessionLock(t.Context(), connection))

	select {
	case <-migrationCtx.Done():
		cause := context.Cause(migrationCtx)
		require.Error(t, cause)
		assert.Contains(t, cause.Error(), "renew remote migration lock")
		assert.True(t, errors.Is(cause, sql.ErrConnDone) || cause.Error() != "")
	case <-time.After(time.Second):
		t.Fatal("renewal did not report the closed database")
	}

	err = locker.SessionUnlock(t.Context(), connection)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "renew remote migration lock")
}

func openDatabase(t *testing.T) (*sql.DB, *sql.Conn) {
	t.Helper()

	database := openPath(t, filepath.Join(t.TempDir(), "remote-lock.db"))
	connection, err := database.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := connection.Close()
		if err != nil && !errors.Is(err, sql.ErrConnDone) {
			require.NoError(t, err)
		}
	})

	return database, connection
}

func openPath(t *testing.T, path string) *sql.DB {
	t.Helper()

	database, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", path))
	require.NoError(t, err)
	database.SetMaxOpenConns(4)
	t.Cleanup(func() {
		if err := database.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			require.NoError(t, err)
		}
	})

	return database
}

func createLockTable(t *testing.T, database *sql.DB) {
	t.Helper()

	_, err := database.ExecContext(t.Context(), `CREATE TABLE cord_migration_lock (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		owner TEXT NOT NULL,
		expires_at INTEGER NOT NULL
	)`)
	require.NoError(t, err)
}

func lockRows(t *testing.T, database *sql.DB) int {
	t.Helper()

	var count int
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM cord_migration_lock").Scan(&count))

	return count
}
