package cord_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()

	dsn := "file:" + filepath.Join(t.TempDir(), "cord.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
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

func newRuntime(t *testing.T) (*sql.DB, *cord.Cord) {
	t.Helper()

	database := openSQLite(t)
	runtime, err := cord.New(database)
	require.NoError(t, err)

	return database, runtime
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

func TestNew_CreatesRuntime(t *testing.T) {
	t.Parallel()
	assert.NotNil(t, mustRuntime(t))
}

func TestWorkflow_RunLinearChain(t *testing.T) {
	t.Parallel()
	flow := mustRuntime(t).From(addOne).Then(isThree)
	result, err := flow.Run(t.Context(), 2)
	require.NoError(t, err)
	assert.True(t, result)
}

func TestWorkflow_RunJoinedBranches(t *testing.T) {
	t.Parallel()
	root := mustRuntime(t).From(timesTwo)
	flow := cord.Join(root.Then(leftText), root.Then(addOne)).Then(formatJoined)
	result, err := flow.Run(t.Context(), 2)
	require.NoError(t, err)
	assert.Equal(t, "left:5", result)
}

func TestWorkflow_RunPropagatesNodeError(t *testing.T) {
	t.Parallel()
	result, err := mustRuntime(t).From(failStep).Then(addOne).Run(t.Context(), 1)
	assert.Zero(t, result)
	require.ErrorIs(t, err, errStepFailed)
}

func TestJoin_UnrelatedWorkflowsFailAtRun(t *testing.T) {
	t.Parallel()
	runtime := mustRuntime(t)
	joined := cord.Join(runtime.From(passThrough), runtime.From(passThrough)).Then(sum)
	result, err := joined.Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: cannot join unrelated workflows")
}

func TestWorkflow_NilStepsFailAtRun(t *testing.T) {
	t.Parallel()
	runtime := mustRuntime(t)

	var rootStep func(context.Context, int) (int, error)

	result, err := runtime.From(rootStep).Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: root step is nil")

	root := runtime.From(passThrough)

	var nextStep func(context.Context, int) (string, error)

	_, err = root.Then(nextStep).Run(t.Context(), 1)
	require.EqualError(t, err, "cord: workflow step is nil")
}

func TestWorkflow_RunRejectsNilContextBeforePersistence(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)

	var ctx context.Context

	result, err := runtime.From(passThrough).Run(ctx, 1)
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

	result, err := mustRuntime(t).From(passThrough).Run(ctx, 1)
	assert.Zero(t, result)
	require.ErrorIs(t, err, context.Canceled)
}

func TestWorkflow_ZeroValueFailsWithoutPanic(t *testing.T) {
	t.Parallel()

	var flow cord.Workflow[int, int]

	result, err := flow.Then(passThrough).Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: invalid workflow")
}
