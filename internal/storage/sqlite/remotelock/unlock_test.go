package remotelock_test

import (
	"database/sql"
	"github.com/omarluq/cord/internal/storage/sqlite/remotelock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

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
