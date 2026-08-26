package cord_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	// Register the pure-Go SQLite driver used by the tests.
	_ "modernc.org/sqlite"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()

	dsn := "file:" + filepath.Join(t.TempDir(), "cord.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	return database
}

func mustRuntime(t *testing.T) *cord.Cord {
	t.Helper()

	_, runtime := newRuntime(t)

	return runtime
}

func newRuntime(t *testing.T, options ...cord.Options) (*sql.DB, *cord.Cord) {
	t.Helper()

	database := openSQLite(t)

	return database, newRuntimeForDB(t, database, options...)
}

func newRuntimeForDB(t *testing.T, database *sql.DB, options ...cord.Options) *cord.Cord {
	t.Helper()

	runtime, err := cord.New(t.Context(), database, options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	return runtime
}

func addOne(_ context.Context, value int) (int, error)      { return value + 1, nil }
func isThree(_ context.Context, value int) (bool, error)    { return value == 3, nil }
func timesTwo(_ context.Context, value int) (int, error)    { return value * 2, nil }
func passThrough(_ context.Context, value int) (int, error) { return value, nil }
func leftText(_ context.Context, _ int) (string, error)     { return "left", nil }
func sum(_ context.Context, left, right int) (int, error)   { return left + right, nil }
func subtract(_ context.Context, left, right int) (int, error) {
	return left - right, nil
}
func formatJoined(_ context.Context, left string, right int) (string, error) {
	return fmt.Sprintf("%s:%d", left, right), nil
}

var errStepFailed = errors.New("step failed")

func failStep(_ context.Context, value int) (int, error) { return value, errStepFailed }

func completeAfterRelease(ctx context.Context, directory string) (string, error) {
	if err := writeMarker(directory, "started"); err != nil {
		return "", err
	}

	if err := waitForMarker(ctx, directory, "release"); err != nil {
		return "", err
	}

	return "completed", nil
}
