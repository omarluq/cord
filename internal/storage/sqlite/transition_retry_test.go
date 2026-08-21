package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	ncruces "github.com/ncruces/go-sqlite3"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFencedTerminalTransitionRetriesWholeTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		leaseExpiry  func() time.Time
		name         string
		busyAttempts int
		wantAttempts int
		wantAccepted bool
		wantBusy     bool
	}{
		{
			name:         "first attempt is busy",
			busyAttempts: 1,
			leaseExpiry:  func() time.Time { return time.Now().Add(time.Minute) },
			wantAccepted: true,
			wantAttempts: 2,
			wantBusy:     false,
		},
		{
			name:         "multiple attempts are busy",
			busyAttempts: 2,
			leaseExpiry:  func() time.Time { return time.Now().Add(time.Minute) },
			wantAccepted: true,
			wantAttempts: 3,
			wantBusy:     false,
		},
		{
			name:         "expired lease bounds the operation",
			busyAttempts: 1,
			leaseExpiry:  func() time.Time { return time.Now().Add(-time.Second) },
			wantAccepted: false,
			wantAttempts: 0,
			wantBusy:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database, store := newTransitionStore(t)
			attempts := 0
			accepted, transitionErr := sqlite.FencedTerminalTransitionForTest(
				t.Context(), store, test.leaseExpiry(), func(attemptCtx context.Context, transaction *sql.Tx) error {
					attempts++

					_, execErr := transaction.ExecContext(
						attemptCtx, "INSERT INTO transition_attempts (attempt) VALUES (?)", attempts,
					)
					if execErr != nil {
						return fmt.Errorf("record transition attempt: %w", execErr)
					}

					if attempts <= test.busyAttempts {
						return ncruces.BUSY
					}

					return nil
				},
			)

			assert.Equal(t, test.wantAccepted, accepted)
			assert.Equal(t, test.wantAttempts, attempts)
			assert.Equal(t, test.wantBusy, sqlite.IsBusyForTest(transitionErr))
			assert.Equal(t, boolToInt(test.wantAccepted), transitionAttemptCount(t, database))
		})
	}
}

func newTransitionStore(t *testing.T) (*sql.DB, *sqlite.Store) {
	t.Helper()

	database, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	database.SetMaxOpenConns(1)

	_, err = database.ExecContext(t.Context(), "CREATE TABLE transition_attempts (attempt INTEGER NOT NULL)")
	require.NoError(t, err)

	store, err := sqlite.New(database)
	require.NoError(t, err)

	return database, store
}

func transitionAttemptCount(t *testing.T, database *sql.DB) int {
	t.Helper()

	var count int

	err := database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM transition_attempts").Scan(&count)
	require.NoError(t, err)

	return count
}

func boolToInt(value bool) int {
	if value {
		return 1
	}

	return 0
}
