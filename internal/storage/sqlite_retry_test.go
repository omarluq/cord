package storage_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	// Register the SQLite driver with database/sql.
	_ "modernc.org/sqlite"
)

func TestStore_CreateRunStopsRetryingSQLiteContentionOnCancellation(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "locked.db")
	first := openZeroTimeoutDatabase(t, path)
	second := openZeroTimeoutDatabase(t, path)
	require.NoError(t, storage.Migrate(t.Context(), first))

	transaction, err := first.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, transaction.Rollback()) })
	_, err = transaction.ExecContext(t.Context(), "UPDATE cord_runs SET status = status")
	require.NoError(t, err)

	store, err := storage.NewStore(second)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	plan := validPlan(time.Now().UTC(), "contended")
	err = store.CreateRun(ctx, &plan)
	require.Error(t, err)
	require.Error(t, ctx.Err())
}

func openZeroTimeoutDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()

	database, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(0)&_pragma=foreign_keys(1)")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	return database
}
