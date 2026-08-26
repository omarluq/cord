package remotelock_test

import (
	"context"
	"database/sql"
	"github.com/omarluq/cord/internal/storage/sqlite/remotelock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

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
