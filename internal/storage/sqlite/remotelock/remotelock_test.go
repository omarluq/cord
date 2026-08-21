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
	// Register the SQLite driver used by sql.Open in the test helpers.
	_ "modernc.org/sqlite"
)

// TestLockerRejectsInvalidRenewalIntervals verifies invalid renewal intervals do not acquire a lease.
func TestLockerRejectsInvalidRenewalIntervals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval time.Duration
	}{
		{name: "zero", interval: 0},
		{name: "negative", interval: -time.Second},
		{name: "boundary", interval: 19 * time.Second},
		{name: "above boundary", interval: 19*time.Second + time.Nanosecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database, connection := openDatabase(t)
			locker := remotelock.New(database, nil, remotelock.WithRenewalInterval(test.interval))

			err := locker.SessionLock(t.Context(), connection)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid remote migration lock renewal interval")
			assert.Equal(t, 0, lockRowsIfTableExists(t, database))
		})
	}
}

// TestLockerRejectsSingleConnectionPool verifies renewal cannot self-block behind Goose's connection.
func TestLockerRejectsSingleConnectionPool(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	database.SetMaxOpenConns(1)

	err := remotelock.New(database, nil).SessionLock(t.Context(), connection)
	require.ErrorIs(t, err, remotelock.ErrInsufficientPoolCapacity)
	database.SetMaxOpenConns(4)
	assert.Equal(t, 0, lockRowsIfTableExists(t, database))
}

// TestLockerAcquiresAndReleases verifies a lease can be acquired and released.
func TestLockerAcquiresAndReleases(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	locker := remotelock.New(database, nil)

	require.NoError(t, locker.SessionLock(t.Context(), connection))
	assert.Equal(t, 1, lockRows(t, database))

	require.NoError(t, locker.SessionUnlock(t.Context(), connection))
	assert.Equal(t, 0, lockRows(t, database))
}

// TestLockerContentionHonorsCancellation verifies waiting lease acquisition observes cancellation.
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

// TestLockerTakesOverAbandonedLease verifies an expired lease can be replaced.
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

// TestLockerReportsRenewalOwnershipLoss verifies ownership loss cancels the migration.
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

// TestLockerReportsOwnershipLossOnUnlock verifies release reports a replaced lease.
func TestLockerReportsOwnershipLossOnUnlock(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	locker := remotelock.New(database, nil)
	require.NoError(t, locker.SessionLock(t.Context(), connection))
	_, err := database.ExecContext(t.Context(), "UPDATE cord_migration_lock SET owner = 'replacement' WHERE id = 1")
	require.NoError(t, err)

	err = locker.SessionUnlock(t.Context(), connection)
	require.ErrorIs(t, err, remotelock.ErrLockOwnershipLost)

	// Failed release still finalizes the lifecycle, so reuse is deterministic.
	require.NoError(t, locker.SessionUnlock(t.Context(), connection))
	_, err = database.ExecContext(t.Context(), "UPDATE cord_migration_lock SET expires_at = 0")
	require.NoError(t, err)
	require.NoError(t, locker.SessionLock(t.Context(), connection))
	require.NoError(t, locker.SessionUnlock(t.Context(), connection))
}

// TestLockerUnlockWithoutLockIsNoOp verifies releasing an unacquired lease succeeds.
func TestLockerUnlockWithoutLockIsNoOp(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	require.NoError(t, connection.Close())
	require.NoError(t, remotelock.New(database, nil).SessionUnlock(t.Context(), connection))
}

// TestLockerReturnsClosedConnectionErrors verifies connection failures are returned.
func TestLockerReturnsClosedConnectionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation func(*testing.T, *sql.DB, *sql.Conn) error
		wantError string
	}{
		{
			name: "acquire",
			operation: func(t *testing.T, database *sql.DB, connection *sql.Conn) error {
				t.Helper()
				require.NoError(t, connection.Close())

				return remotelock.New(database, nil).SessionLock(t.Context(), connection)
			},
			wantError: "create remote migration lock table",
		},
		{
			name: "release",
			operation: func(t *testing.T, database *sql.DB, connection *sql.Conn) error {
				t.Helper()

				locker := remotelock.New(database, nil)
				require.NoError(t, locker.SessionLock(t.Context(), connection))
				require.NoError(t, connection.Close())

				return locker.SessionUnlock(t.Context(), connection)
			},
			wantError: "release remote migration lock",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database, connection := openDatabase(t)
			err := test.operation(t, database, connection)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantError)
		})
	}
}

// TestLockerUnlockJoinsRenewalAndReleaseErrors verifies cleanup preserves both failures.
func TestLockerUnlockJoinsRenewalAndReleaseErrors(t *testing.T) {
	t.Parallel()

	database, connection := openDatabase(t)
	createLockTable(t, database)
	_, err := database.ExecContext(t.Context(), `CREATE TRIGGER reject_renewal_and_release
		BEFORE UPDATE ON cord_migration_lock
		BEGIN SELECT RAISE(IGNORE); END`)
	require.NoError(t, err)

	migrationCtx, cancelMigration := context.WithCancelCause(t.Context())
	locker := remotelock.New(database, cancelMigration, remotelock.WithRenewalInterval(10*time.Millisecond))
	require.NoError(t, locker.SessionLock(t.Context(), connection))

	select {
	case <-migrationCtx.Done():
		require.ErrorIs(t, context.Cause(migrationCtx), remotelock.ErrLockOwnershipLost)
	case <-time.After(time.Second):
		t.Fatal("renewal did not report ownership loss")
	}

	require.NoError(t, connection.Close())
	err = locker.SessionUnlock(t.Context(), connection)
	require.ErrorIs(t, err, remotelock.ErrLockOwnershipLost)
	require.ErrorIs(t, err, sql.ErrConnDone)
	require.NoError(t, locker.SessionUnlock(t.Context(), connection))
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

func lockRowsIfTableExists(t *testing.T, database *sql.DB) int {
	t.Helper()

	var count int
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'table' AND name = 'cord_migration_lock'`).Scan(&count))

	return count
}

func lockRows(t *testing.T, database *sql.DB) int {
	t.Helper()

	var count int
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM cord_migration_lock").Scan(&count))

	return count
}
