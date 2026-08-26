package cord_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const retryWaitStatus = "retry_wait"

func alwaysFails(_ context.Context, value int) (int, error) {
	return value, errors.New("still failing")
}

func failsPermanently(_ context.Context, value int) (int, error) {
	return value, fmt.Errorf("marked failure: %w", cord.Permanent(errStepFailed))
}

func succeedsWhenReleased(_ context.Context, directory string) (string, error) {
	if _, err := os.Stat(filepath.Join(directory, "release")); err != nil {
		return "", errors.New("not released")
	}

	return "recovered", nil
}

func blockFairnessRun(ctx context.Context, directory string) (string, error) {
	if err := writeMarker(directory, "active-started"); err != nil {
		return "", err
	}

	if err := waitForMarker(ctx, directory, "release-active"); err != nil {
		return "", err
	}

	return directory, nil
}

func continueFairnessRun(_ context.Context, directory string) (string, error) {
	if _, err := os.Stat(filepath.Join(directory, "waiting-completed")); err != nil {
		return "", errors.New("waiting workflow was starved")
	}

	return "active", nil
}

func completeWaitingRun(_ context.Context, directory string) (string, error) {
	if err := writeMarker(directory, "waiting-completed"); err != nil {
		return "", err
	}

	return "waiting", nil
}

func TestScheduler_DoesNotStarveWaitingWorkflow(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t, cord.Options{Concurrency: 1})
	directory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(directory, "release-active")) })

	activeResult := make(chan workflowResult, 1)

	go func() {
		value, err := runtime.From("active-workflow", blockFairnessRun).
			Then(continueFairnessRun).
			Run(t.Context(), directory)
		activeResult <- workflowResult{value: value, err: err}
	}()

	require.NoError(t, waitForMarker(t.Context(), directory, "active-started"))

	waitingResult := make(chan workflowResult, 1)

	go func() {
		value, err := runtime.From("waiting-workflow", completeWaitingRun).Run(t.Context(), directory)
		waitingResult <- workflowResult{value: value, err: err}
	}()

	require.Eventually(t, func() bool {
		var count int

		err := database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_runs").Scan(&count)

		return err == nil && count == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, writeMarker(directory, "release-active"))

	waiting := waitWorkflowResult(t, waitingResult)
	require.NoError(t, waiting.err)
	assert.Equal(t, "waiting", waiting.value)

	active := waitWorkflowResult(t, activeResult)
	require.NoError(t, active.err)
	assert.Equal(t, "active", active.value)
}
