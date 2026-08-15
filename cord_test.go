package cord_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
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

func TestNew_CreatesRuntime(t *testing.T) {
	t.Parallel()
	assert.NotNil(t, mustRuntime(t))
}

func TestWorkflow_RunLinearChain(t *testing.T) {
	t.Parallel()
	flow := mustRuntime(t).From("test-workflow", addOne).Then(isThree)
	result, err := flow.Run(t.Context(), 2)
	require.NoError(t, err)
	assert.True(t, result)
}

func TestWorkflow_RunJoinedBranches(t *testing.T) {
	t.Parallel()
	root := mustRuntime(t).From("test-workflow", timesTwo)
	flow := cord.Join(root.Then(leftText), root.Then(addOne)).Then(formatJoined)
	result, err := flow.Run(t.Context(), 2)
	require.NoError(t, err)
	assert.Equal(t, "left:5", result)
}

func TestWorkflow_RunPropagatesNodeError(t *testing.T) {
	t.Parallel()
	result, err := mustRuntime(t).From("test-workflow", failStep).Then(addOne).Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, errStepFailed.Error())
}

func TestJoin_UnrelatedWorkflowsFailAtRun(t *testing.T) {
	t.Parallel()
	runtime := mustRuntime(t)
	left := runtime.From("left-workflow", passThrough)
	right := runtime.From("right-workflow", passThrough)
	joined := cord.Join(left, right).Then(sum)
	result, err := joined.Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: cannot join unrelated workflows")
}

func TestWorkflow_EmptyNameFailsAtRun(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)

	result, err := runtime.From("", passThrough).Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: workflow name is empty")

	var runs int
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_runs").Scan(&runs))
	assert.Zero(t, runs)
}

func TestWorkflow_NilStepsFailAtRun(t *testing.T) {
	t.Parallel()
	runtime := mustRuntime(t)

	var rootStep func(context.Context, int) (int, error)

	result, err := runtime.From("test-workflow", rootStep).Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: root step is nil")

	root := runtime.From("test-workflow", passThrough)

	var nextStep func(context.Context, int) (string, error)

	_, err = root.Then(nextStep).Run(t.Context(), 1)
	require.EqualError(t, err, "cord: workflow step is nil")
}

func TestWorkflow_RunRejectsNilContextBeforePersistence(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)

	var ctx context.Context

	result, err := runtime.From("test-workflow", passThrough).Run(ctx, 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: workflow context is nil")

	var runs int
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_runs").Scan(&runs))
	assert.Zero(t, runs)
}

func TestWorkflow_RunWithCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := mustRuntime(t).From("test-workflow", passThrough).Run(ctx, 1)
	assert.Zero(t, result)
	require.ErrorIs(t, err, context.Canceled)
}

func TestWorkflow_RunContextCancellationStopsWaitingWithoutCancelingDurableRun(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)
	directory := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)

	go func() {
		_, err := runtime.From("context-cancellation", completeAfterRelease).Run(ctx, directory)
		result <- err
	}()

	require.NoError(t, waitForMarker(t.Context(), directory, "started"))
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)

	var status string
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		"SELECT status FROM cord_runs WHERE workflow_name = ?",
		"context-cancellation",
	).Scan(&status))
	assert.Equal(t, "running", status)

	require.NoError(t, writeMarker(directory, "release"))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err := database.QueryRowContext(
			t.Context(),
			"SELECT status FROM cord_runs WHERE workflow_name = ?",
			"context-cancellation",
		).Scan(&status)
		require.NoError(collect, err)
		assert.Equal(collect, "completed", status)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestWorkflow_ZeroValueFailsWithoutPanic(t *testing.T) {
	t.Parallel()

	var flow cord.Workflow[int, int]

	result, err := flow.Then(passThrough).Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: invalid workflow")
}
