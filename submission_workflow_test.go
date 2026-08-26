package cord_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errAsyncPersisted = errors.New("persisted async failure")

func failAsyncPermanently(_ context.Context, value int) (int, error) {
	return value, fmt.Errorf("async step: %w", cord.Permanent(errAsyncPersisted))
}

type asyncSubmitResult struct {
	err   error
	id    cord.RunID
	input int
}

func concurrentCancelResults(callers int, cancel func() error) []error {
	results := make([]error, callers)
	start := make(chan struct{})

	var wait sync.WaitGroup
	wait.Add(callers)

	for index := range callers {
		go func() {
			defer wait.Done()

			<-start

			results[index] = cancel()
		}()
	}

	close(start)
	wait.Wait()

	return results
}

func submitBlockedQueuedRun(
	t *testing.T,
	workflowName string,
) (*cord.Cord, cord.Workflow[string, string], cord.RunID) {
	t.Helper()

	_, runtime := newRuntime(t, cord.Options{Concurrency: 1})
	flow := runtime.From(workflowName, completeAfterRelease)
	activeDirectory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(activeDirectory, "release")) })

	_, err := flow.Submit(t.Context(), activeDirectory)
	require.NoError(t, err)
	waitMarker(t, activeDirectory, "started")

	runID, err := flow.Submit(t.Context(), t.TempDir())
	require.NoError(t, err)

	return runtime, flow, runID
}

func TestWorkflowSubmitAndGet(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-submit-get", addOne)
	runID, err := flow.Submit(t.Context(), 41)
	require.NoError(t, err)

	parsed, err := uuid.FromString(string(runID))
	require.NoError(t, err)
	assert.Equal(t, byte(7), parsed.Version())

	result, err := flow.Get(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}

func TestWorkflowSubmitReturnsWhileExecutionIsBlocked(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(directory, "release")) })
	flow := mustRuntime(t).From("async-submit-is-nonblocking", completeAfterRelease)

	runID, err := flow.Submit(t.Context(), directory)
	require.NoError(t, err)
	require.NotEmpty(t, runID)
	waitMarker(t, directory, "started")

	_, err = os.Stat(filepath.Join(directory, "release"))
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, writeMarker(directory, "release"))
	result, err := flow.Get(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, "completed", result)
}

func TestWorkflowSubmitIdempotency(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-idempotency", addOne)
	first, err := flow.Submit(t.Context(), 1, "request-1")
	require.NoError(t, err)
	second, err := flow.Submit(t.Context(), 1, "request-1")
	require.NoError(t, err)
	assert.Equal(t, first, second)

	_, err = flow.Submit(t.Context(), 2, "request-1")
	require.ErrorIs(t, err, cord.ErrRunConflict)

	unkeyedFirst, err := flow.Submit(t.Context(), 1)
	require.NoError(t, err)
	unkeyedSecond, err := flow.Submit(t.Context(), 1)
	require.NoError(t, err)
	assert.NotEqual(t, unkeyedFirst, unkeyedSecond)
}

func TestWorkflowConcurrentKeyedSubmissionsConvergeAcrossRuntimes(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	firstRuntime := newRuntimeForDB(t, database)
	secondRuntime := newRuntimeForDB(t, database)
	flows := []cord.Workflow[int, int]{
		firstRuntime.From("async-concurrent-convergence", addOne),
		secondRuntime.From("async-concurrent-convergence", addOne),
	}

	const callers = 24

	results := make(chan asyncSubmitResult, callers)
	start := make(chan struct{})

	var wait sync.WaitGroup
	wait.Add(callers)

	for index := range callers {
		go func() {
			defer wait.Done()

			<-start

			id, err := flows[index%len(flows)].Submit(t.Context(), 8, "shared-request")
			results <- asyncSubmitResult{id: id, err: err, input: 8}
		}()
	}

	close(start)
	wait.Wait()
	close(results)

	var converged cord.RunID

	for result := range results {
		require.NoError(t, result.err)

		if converged == "" {
			converged = result.id
		}

		assert.Equal(t, converged, result.id)
	}

	value, err := flows[1].Get(t.Context(), converged)
	require.NoError(t, err)
	assert.Equal(t, 9, value)
}
