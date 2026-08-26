package remotelock_test

import (
	"context"
	"github.com/omarluq/cord/internal/storage/sqlite/remotelock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
