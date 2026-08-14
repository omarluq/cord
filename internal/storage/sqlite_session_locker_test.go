package storage_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLiteSessionLockerBlocksOtherConnectionsUntilUnlock(t *testing.T) {
	t.Parallel()

	database := openSessionLockerDatabase(t)
	lockerConnection, err := database.Conn(t.Context())
	require.NoError(t, err)

	defer func() { require.NoError(t, lockerConnection.Close()) }()

	otherConnection, err := database.Conn(t.Context())
	require.NoError(t, err)

	defer func() { require.NoError(t, otherConnection.Close()) }()

	locker := storage.SQLiteSessionLocker{}
	require.NoError(t, locker.SessionLock(t.Context(), lockerConnection))

	_, err = otherConnection.ExecContext(t.Context(), "CREATE TABLE blocked_while_locked (id INTEGER)")
	require.Error(t, err)

	require.NoError(t, locker.SessionUnlock(t.Context(), lockerConnection))
	_, err = otherConnection.ExecContext(t.Context(), "CREATE TABLE allowed_after_unlock (id INTEGER)")
	require.NoError(t, err)
}

func TestSQLiteSessionLockerReportsConnectionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation func(context.Context, storage.SQLiteSessionLocker, *sql.Conn) error
		message   string
	}{
		{
			name: "lock",
			operation: func(ctx context.Context, locker storage.SQLiteSessionLocker, connection *sql.Conn) error {
				return locker.SessionLock(ctx, connection)
			},
			message: "initialize normal sqlite locking",
		},
		{
			name: "unlock",
			operation: func(ctx context.Context, locker storage.SQLiteSessionLocker, connection *sql.Conn) error {
				return locker.SessionUnlock(ctx, connection)
			},
			message: "restore normal sqlite locking",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database := openSessionLockerDatabase(t)
			connection, err := database.Conn(t.Context())
			require.NoError(t, err)
			require.NoError(t, connection.Close())

			err = test.operation(t.Context(), storage.SQLiteSessionLocker{}, connection)
			require.ErrorIs(t, err, sql.ErrConnDone)
			assert.Contains(t, err.Error(), test.message)
		})
	}
}

func TestSQLiteSessionLockerHandlesContention(t *testing.T) {
	t.Parallel()

	blockingConnection, waitingConnection := openContendedSessionLockerConnections(t)

	err := (storage.SQLiteSessionLocker{}).SessionLock(t.Context(), waitingConnection)
	require.Error(t, err)

	releaseExclusiveLock(t, blockingConnection)

	var mode string
	require.NoError(t, waitingConnection.QueryRowContext(t.Context(), "PRAGMA locking_mode").Scan(&mode))
	assert.Equal(t, "normal", mode)
}

func TestSQLiteSessionLockerReleasesWALLock(t *testing.T) {
	t.Parallel()

	database := openSessionLockerDatabase(t)
	_, err := database.ExecContext(t.Context(), "PRAGMA journal_mode = WAL")
	require.NoError(t, err)

	lockerConnection, err := database.Conn(t.Context())
	require.NoError(t, err)

	defer func() { require.NoError(t, lockerConnection.Close()) }()

	otherConnection, err := database.Conn(t.Context())
	require.NoError(t, err)

	defer func() { require.NoError(t, otherConnection.Close()) }()

	locker := storage.SQLiteSessionLocker{}
	require.NoError(t, locker.SessionLock(t.Context(), lockerConnection))
	require.NoError(t, locker.SessionUnlock(t.Context(), lockerConnection))

	var schemaEntries int
	require.NoError(t,
		otherConnection.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_schema").Scan(&schemaEntries),
	)
}

func TestSQLiteSessionLockerReturnsWhenLockWaitIsCanceled(t *testing.T) {
	t.Parallel()

	blockingConnection, waitingConnection := openContendedSessionLockerConnections(t)
	defer releaseExclusiveLock(t, blockingConnection)

	ctx, cancel := context.WithCancel(t.Context())

	result := make(chan error, 1)
	go func() {
		result <- (storage.SQLiteSessionLocker{}).SessionLock(ctx, waitingConnection)
	}()

	var err error
	select {
	case err = <-result:
		require.Failf(t, "SessionLock returned before cancellation", "error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	err = <-result
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWrapSQLiteRollbackError(t *testing.T) {
	t.Parallel()

	cause := errors.New("rollback failed")
	tests := []struct {
		name    string
		cause   error
		message string
	}{
		{name: "nil", cause: nil, message: ""},
		{
			name:    "rollback failure",
			cause:   cause,
			message: "rollback sqlite migration lock transaction",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := storage.WrapSQLiteRollbackError(test.cause)
			if test.cause == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, test.cause)
			assert.Contains(t, err.Error(), test.message)
		})
	}
}

func openContendedSessionLockerConnections(
	t *testing.T,
) (blockingConnection, waitingConnection *sql.Conn) {
	t.Helper()

	database := openSessionLockerDatabase(t)
	blockingConnection, err := database.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, blockingConnection.Close()) })

	waitingConnection, err = database.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, waitingConnection.Close()) })

	_, err = blockingConnection.ExecContext(t.Context(), "BEGIN EXCLUSIVE")
	require.NoError(t, err)

	return blockingConnection, waitingConnection
}

func releaseExclusiveLock(t *testing.T, connection *sql.Conn) {
	t.Helper()

	_, err := connection.ExecContext(context.WithoutCancel(t.Context()), "ROLLBACK")
	require.NoError(t, err)
}

func openSessionLockerDatabase(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "session-locker.db")
	database, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(0)")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	return database
}
