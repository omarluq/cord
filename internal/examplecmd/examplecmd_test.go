package examplecmd_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/omarluq/cord/internal/examplecmd"
	"github.com/omarluq/cord/internal/exampledb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	t.Parallel()

	var processState sync.Mutex

	type contextKey struct{}

	t.Run("executes workflow, prints result, and closes database", func(t *testing.T) {
		t.Parallel()
		processState.Lock()
		defer processState.Unlock()

		testSuccess(t, contextKey{})
	})

	t.Run("returns open error without running workflow", func(t *testing.T) {
		t.Parallel()
		processState.Lock()
		defer processState.Unlock()

		openErr := errors.New("open failed")
		workflowCalled := false
		err := examplecmd.Run(t.Context(), func(context.Context) (*sql.DB, error) {
			return nil, openErr
		}, func(context.Context, *sql.DB, int) (int, error) {
			workflowCalled = true

			return 0, nil
		}, 1)

		require.ErrorIs(t, err, openErr)
		assert.False(t, workflowCalled)
	})

	t.Run("wraps workflow error and closes database", func(t *testing.T) {
		t.Parallel()
		processState.Lock()
		defer processState.Unlock()

		workflowErr := errors.New("workflow failed")
		database := exampledb.DB()
		err := examplecmd.Run(t.Context(), func(context.Context) (*sql.DB, error) {
			return database, nil
		}, func(context.Context, *sql.DB, int) (int, error) {
			return 0, workflowErr
		}, 1)

		require.ErrorIs(t, err, workflowErr)
		require.ErrorContains(t, err, "run workflow")
		require.Error(t, database.PingContext(t.Context()))
	})

	t.Run("reports output failure and closes database", func(t *testing.T) {
		t.Parallel()
		processState.Lock()
		defer processState.Unlock()

		database := exampledb.DB()
		output, err := os.CreateTemp(t.TempDir(), "closed-output")
		require.NoError(t, err)
		require.NoError(t, output.Close())

		original := os.Stdout

		os.Stdout = output
		defer func() { os.Stdout = original }()

		err = examplecmd.Run(t.Context(), func(context.Context) (*sql.DB, error) {
			return database, nil
		}, func(context.Context, *sql.DB, int) (string, error) {
			return "result", nil
		}, 1)

		require.ErrorContains(t, err, "write result")
		require.Error(t, database.PingContext(t.Context()))
	})
}

func testSuccess(t *testing.T, key any) {
	t.Helper()

	database := exampledb.DB()
	ctx := context.WithValue(t.Context(), key, "request-value")

	output, err := captureStdout(t, func() error {
		return examplecmd.Run(ctx, func(got context.Context) (*sql.DB, error) {
			assert.Equal(t, "request-value", got.Value(key))

			return database, nil
		}, func(got context.Context, gotDatabase *sql.DB, input int) (int, error) {
			assert.Equal(t, "request-value", got.Value(key))
			assert.Same(t, database, gotDatabase)
			assert.Equal(t, 7, input)

			return input * 2, nil
		}, 7)
	})

	require.NoError(t, err)
	assert.Equal(t, "14\n", output)
	require.Error(t, database.PingContext(t.Context()))
}

func captureStdout(t *testing.T, action func() error) (string, error) {
	t.Helper()

	reader, writer, err := os.Pipe()
	require.NoError(t, err)

	original := os.Stdout
	os.Stdout = writer
	actionErr := action()
	os.Stdout = original

	require.NoError(t, writer.Close())

	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	return string(output), actionErr
}
