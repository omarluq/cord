package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	mattn "github.com/mattn/go-sqlite3"
	ncruces "github.com/ncruces/go-sqlite3"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
	// Register modernc's database/sql driver for the integration case below.
	_ "modernc.org/sqlite"
)

func TestIsBusy(t *testing.T) {
	t.Parallel()

	moderncBusy := moderncBusyError(t)
	tests := []struct {
		err  error
		name string
		want bool
	}{
		{name: "mattn primary value", err: mattn.ErrBusy, want: true},
		{name: "mattn extended value", err: mattn.ErrBusySnapshot, want: true},
		{name: "mattn primary struct", err: mattn.Error{Code: mattn.ErrBusy}, want: true},
		{
			name: "mattn extended struct",
			err:  mattn.Error{Code: mattn.ErrBusy, ExtendedCode: mattn.ErrBusySnapshot},
			want: true,
		},
		{name: "ncruces primary", err: ncruces.BUSY, want: true},
		{name: "ncruces extended", err: ncruces.BUSY_SNAPSHOT, want: true},
		{name: "modernc", err: moderncBusy, want: true},
		{name: "wrapped", err: fmt.Errorf("wrapped: %w", ncruces.BUSY), want: true},
		{name: "joined", err: errors.Join(errors.New("other"), moderncBusy), want: true},
		{name: "mattn other value", err: mattn.ErrConstraint, want: false},
		{name: "mattn other struct", err: mattn.Error{Code: mattn.ErrConstraint}, want: false},
		{name: "ncruces other", err: ncruces.CONSTRAINT, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, sqlite.IsBusyForTest(test.err))
		})
	}
}

func moderncBusyError(t *testing.T) error {
	t.Helper()

	path := filepath.Join(t.TempDir(), "busy.db")
	first := openModerncDatabase(t, path)
	second := openModerncDatabase(t, path)

	_, err := first.ExecContext(t.Context(), "CREATE TABLE locks (value INTEGER)")
	require.NoError(t, err)

	transaction, err := first.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			t.Errorf("rollback transaction: %v", rollbackErr)
		}
	})

	_, err = transaction.ExecContext(t.Context(), "INSERT INTO locks VALUES (1)")
	require.NoError(t, err)

	_, err = second.ExecContext(context.Background(), "INSERT INTO locks VALUES (2)")
	require.Error(t, err)

	return fmt.Errorf("produce busy error: %w", err)
}

func openModerncDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()

	database, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(0)")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	return database
}
