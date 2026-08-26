package cord_test

import (
	"context"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Register the pure-Go SQLite driver used by the tests.
	_ "modernc.org/sqlite"
)

func TestWorkflow_RunLinearChain(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("test-workflow", addOne).Then(isThree)
	result, err := flow.Run(t.Context(), 2)
	require.NoError(t, err)
	assert.True(t, result)
}

func TestWorkflow_RunPropagatesNodeError(t *testing.T) {
	t.Parallel()

	result, err := mustRuntime(t).From("test-workflow", failStep).Then(addOne).Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, errStepFailed.Error())
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
