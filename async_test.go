package cord_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestWorkflowConcurrentKeyedSubmissionsConflictDeterministically(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-concurrent-conflict", addOne)

	const callers = 24

	results := make(chan asyncSubmitResult, callers)
	start := make(chan struct{})

	var wait sync.WaitGroup
	wait.Add(callers)

	for index := range callers {
		go func() {
			defer wait.Done()

			input := index%2 + 1

			<-start

			id, err := flow.Submit(t.Context(), input, "contended-request")
			results <- asyncSubmitResult{id: id, err: err, input: input}
		}()
	}

	close(start)
	wait.Wait()
	close(results)

	winnerInput := 0

	var winnerID cord.RunID

	conflicts := make([]asyncSubmitResult, 0, callers/2)

	for result := range results {
		if result.err != nil {
			require.ErrorIs(t, result.err, cord.ErrRunConflict)
			conflicts = append(conflicts, result)

			continue
		}

		if winnerInput == 0 {
			winnerInput, winnerID = result.input, result.id
		}

		assert.Equal(t, winnerInput, result.input)
		assert.Equal(t, winnerID, result.id)
	}

	require.NotZero(t, winnerInput)
	require.NotEmpty(t, conflicts)

	for _, conflict := range conflicts {
		assert.NotEqual(t, winnerInput, conflict.input)
	}

	value, err := flow.Get(t.Context(), winnerID)
	require.NoError(t, err)
	assert.Equal(t, winnerInput+1, value)
}

func TestWorkflowIdempotencyScopesDefinitionAndName(t *testing.T) {
	t.Parallel()

	runtime := mustRuntime(t)
	original := runtime.From("async-definition-scope", addOne)
	originalID, err := original.Submit(t.Context(), 3, "same-key")
	require.NoError(t, err)

	changed := runtime.From("async-definition-scope", timesTwo)
	_, err = changed.Submit(t.Context(), 3, "same-key")
	require.ErrorIs(t, err, cord.ErrRunConflict)

	otherName := runtime.From("async-name-scope", addOne)
	otherID, err := otherName.Submit(t.Context(), 3, "same-key")
	require.NoError(t, err)
	assert.NotEqual(t, originalID, otherID)

	originalResult, err := original.Get(t.Context(), originalID)
	require.NoError(t, err)
	assert.Equal(t, 4, originalResult)
	otherResult, err := otherName.Get(t.Context(), otherID)
	require.NoError(t, err)
	assert.Equal(t, 4, otherResult)
}

func TestWorkflowSubmitValidatesIdempotencyKey(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-key-validation", addOne)
	tests := []struct {
		name string
		keys []string
	}{
		{name: "empty", keys: []string{""}},
		{name: "invalid UTF-8", keys: []string{string([]byte{0xff})}},
		{name: "NUL", keys: []string{"a\x00b"}},
		{name: "overlength", keys: []string{strings.Repeat("x", 256)}},
		{name: "multiple", keys: []string{"one", "two"}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runID, err := flow.Submit(t.Context(), 1, testCase.keys...)
			assert.Empty(t, runID)
			require.Error(t, err)
		})
	}
}

func TestWorkflowGetErrors(t *testing.T) {
	t.Parallel()

	runtime := mustRuntime(t)
	flow := runtime.From("async-get-errors", addOne)

	_, err := flow.Get(t.Context(), "missing")
	require.ErrorIs(t, err, cord.ErrRunNotFound)

	_, err = flow.Get(t.Context(), "")
	require.Error(t, err)

	other := runtime.From("async-get-other", addOne)
	id, err := flow.Submit(t.Context(), 1)
	require.NoError(t, err)
	_, err = other.Get(t.Context(), id)
	assert.ErrorIs(t, err, cord.ErrRunIncompatible)
}

func TestWorkflowGetVerifiesCompleteDefinition(t *testing.T) {
	t.Parallel()

	runtime := mustRuntime(t)
	flow := runtime.From("async-complete-definition", addOne)
	runID, err := flow.Submit(t.Context(), 1)
	require.NoError(t, err)

	// This handle has the same workflow name and terminal signature, but a
	// different compiled graph and terminal identity.
	other := runtime.From("async-complete-definition", passThrough).Then(addOne)
	_, err = other.Get(t.Context(), runID)
	assert.ErrorIs(t, err, cord.ErrRunIncompatible)
}

func TestWorkflowGetUsesPersistedRetryPolicy(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	first := newRuntimeForDB(t, database, cord.Options{
		MaxAttempts: 7, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 2 * time.Millisecond,
	})
	flow := first.From("async-persisted-retry", addOne)
	id, err := flow.Submit(t.Context(), 41)
	require.NoError(t, err)

	second := newRuntimeForDB(t, database)
	result, err := second.From("async-persisted-retry", addOne).Get(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}

func TestWorkflowGetCancellationStopsOnlyPendingWait(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(directory, "release")) })
	flow := mustRuntime(t).From("async-get-context", completeAfterRelease)
	runID, err := flow.Submit(t.Context(), directory)
	require.NoError(t, err)
	waitMarker(t, directory, "started")

	waitCtx, cancel := context.WithCancel(t.Context())
	waitResult := make(chan error, 1)

	go func() {
		_, getErr := flow.Get(waitCtx, runID)
		waitResult <- getErr
	}()

	cancel()

	select {
	case waitErr := <-waitResult:
		require.ErrorIs(t, waitErr, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Get did not stop after its wait context was canceled")
	}

	_, err = os.Stat(filepath.Join(directory, "release"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, writeMarker(directory, "release"))
	result, err := flow.Get(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, "completed", result)
}

func TestWorkflowCancelQueuedRun(t *testing.T) {
	t.Parallel()

	_, runtime := newRuntime(t, cord.Options{Concurrency: 1})
	flow := runtime.From("async-cancel-queued", completeAfterRelease)
	activeDirectory := t.TempDir()
	queuedDirectory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(activeDirectory, "release")) })

	activeID, err := flow.Submit(t.Context(), activeDirectory)
	require.NoError(t, err)
	waitMarker(t, activeDirectory, "started")

	queuedID, err := flow.Submit(t.Context(), queuedDirectory)
	require.NoError(t, err)

	for _, cancelErr := range concurrentCancelResults(8, func() error {
		return flow.Cancel(t.Context(), queuedID)
	}) {
		require.NoError(t, cancelErr)
	}

	_, err = flow.Get(t.Context(), queuedID)
	require.ErrorIs(t, err, cord.ErrRunCanceled)

	require.NoError(t, writeMarker(activeDirectory, "release"))
	_, err = flow.Get(t.Context(), activeID)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(queuedDirectory, "started"))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestWorkflowCancelRunningRun(t *testing.T) {
	t.Parallel()

	_, runtime := newRuntime(t, cord.Options{
		PollInterval: time.Millisecond, LeaseTTL: 100 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond,
	})
	flow := runtime.From("async-cancel-running", completeAfterRelease)
	directory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(directory, "release")) })

	runID, err := flow.Submit(t.Context(), directory)
	require.NoError(t, err)
	waitMarker(t, directory, "started")

	for _, cancelErr := range concurrentCancelResults(8, func() error {
		return flow.Cancel(t.Context(), runID)
	}) {
		require.NoError(t, cancelErr)
	}

	_, err = flow.Get(t.Context(), runID)
	require.ErrorIs(t, err, cord.ErrRunCanceled)
}

func TestWorkflowCancelIsIndependentOfWorkflowGraphValidity(t *testing.T) {
	t.Parallel()

	_, runtime := newRuntime(t, cord.Options{Concurrency: 1})
	flow := runtime.From("async-cancel-run-id-only", completeAfterRelease)
	activeDirectory := t.TempDir()
	queuedDirectory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(activeDirectory, "release")) })

	_, err := flow.Submit(t.Context(), activeDirectory)
	require.NoError(t, err)
	waitMarker(t, activeDirectory, "started")

	runID, err := flow.Submit(t.Context(), queuedDirectory)
	require.NoError(t, err)

	invalid := runtime.From("", completeAfterRelease)

	require.NoError(t, invalid.Cancel(t.Context(), runID))
	_, err = flow.Get(t.Context(), runID)
	require.ErrorIs(t, err, cord.ErrRunCanceled)
}

func TestWorkflowCancelImmediatelyProducesAuthoritativeSnapshot(t *testing.T) {
	t.Parallel()

	_, runtime := newRuntime(t, cord.Options{Concurrency: 1})
	flow := runtime.From("async-cancel-inspect", completeAfterRelease)
	activeDirectory := t.TempDir()
	queuedDirectory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(activeDirectory, "release")) })

	_, err := flow.Submit(t.Context(), activeDirectory)
	require.NoError(t, err)
	waitMarker(t, activeDirectory, "started")

	runID, err := flow.Submit(t.Context(), queuedDirectory)
	require.NoError(t, err)
	require.NoError(t, flow.Cancel(t.Context(), runID))

	report, err := runtime.InspectRun(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, cord.RunStateCanceled, report.State)
	assert.Equal(t, cord.ReasonCanceledByRequest, report.Reason)
	require.NotNil(t, report.FinishedAt)
	assert.Equal(t, report.StateChangedAt, *report.FinishedAt)
	assert.Nil(t, report.TerminalRunnerID)
	assert.Equal(t, 1, report.NodeCounts.Canceled)
	assert.Zero(t, report.NodeCounts.Pending)
	assert.Zero(t, report.NodeCounts.Ready)
	assert.Zero(t, report.NodeCounts.Running)
	assert.Zero(t, report.NodeCounts.RetryWait)

	page, err := runtime.ListRunNodes(t.Context(), runID, cord.NodeQuery{PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 1)
	assert.Equal(t, cord.NodeStateCanceled, page.Nodes[0].State)
	assert.Equal(t, cord.ReasonCanceledByRequest, page.Nodes[0].Reason)
	assert.Empty(t, page.ContinuationToken)
}

func TestWorkflowCancellationConvergesAcrossCallerCounts(t *testing.T) {
	t.Parallel()

	for _, callers := range []int{1, 2, 8, 100} {
		t.Run(strconv.Itoa(callers), func(t *testing.T) {
			t.Parallel()

			_, runtime := newRuntime(t, cord.Options{Concurrency: 1})
			flow := runtime.From(fmt.Sprintf("async-cancel-callers-%d", callers), completeAfterRelease)
			activeDirectory := t.TempDir()
			queuedDirectory := t.TempDir()
			t.Cleanup(func() { assert.NoError(t, writeMarker(activeDirectory, "release")) })

			_, err := flow.Submit(t.Context(), activeDirectory)
			require.NoError(t, err)
			waitMarker(t, activeDirectory, "started")
			runID, err := flow.Submit(t.Context(), queuedDirectory)
			require.NoError(t, err)

			for _, cancelErr := range concurrentCancelResults(callers, func() error {
				return flow.Cancel(t.Context(), runID)
			}) {
				require.NoError(t, cancelErr)
			}

			report, inspectErr := runtime.InspectRun(t.Context(), runID)
			require.NoError(t, inspectErr)
			assert.Equal(t, cord.RunStateCanceled, report.State)
		})
	}
}

func TestWorkflowCancelCompletedRun(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-cancel-completed", addOne)
	runID, err := flow.Submit(t.Context(), 1)
	require.NoError(t, err)
	_, err = flow.Get(t.Context(), runID)
	require.NoError(t, err)

	for _, cancelErr := range concurrentCancelResults(8, func() error {
		return flow.Cancel(t.Context(), runID)
	}) {
		assert.ErrorIs(t, cancelErr, cord.ErrRunFinished)
	}
}

func TestWorkflowCancelErrors(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-cancel-errors", addOne)
	require.ErrorIs(t, flow.Cancel(t.Context(), "missing"), cord.ErrRunNotFound)
	require.Error(t, flow.Cancel(t.Context(), ""))
}

func TestWorkflowAsyncMethodsRejectNilContexts(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-nil-context", addOne)

	var ctx context.Context

	runID, err := flow.Submit(ctx, 1)
	assert.Empty(t, runID)
	require.ErrorContains(t, err, "context is nil")

	result, err := flow.Get(ctx, "run-id")
	assert.Zero(t, result)
	require.ErrorContains(t, err, "context is nil")

	require.ErrorContains(t, flow.Cancel(ctx, "run-id"), "context is nil")
}

func TestWorkflowAsyncOperationsAcrossRuntimes(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	options := cord.Options{
		PollInterval:      time.Millisecond,
		LeaseTTL:          time.Second,
		HeartbeatInterval: 50 * time.Millisecond,
	}
	firstRuntime := newRuntimeForDB(t, database, options)
	secondRuntime := newRuntimeForDB(t, database, options)
	firstFlow := firstRuntime.From("async-two-runtime", completeAfterRelease)
	secondFlow := secondRuntime.From("async-two-runtime", completeAfterRelease)

	completedDirectory := t.TempDir()
	completedID, err := firstFlow.Submit(t.Context(), completedDirectory)
	require.NoError(t, err)
	waitMarker(t, completedDirectory, "started")
	require.NoError(t, writeMarker(completedDirectory, "release"))
	result, err := secondFlow.Get(t.Context(), completedID)
	require.NoError(t, err)
	assert.Equal(t, "completed", result)

	canceledDirectory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(canceledDirectory, "release")) })
	canceledID, err := firstFlow.Submit(t.Context(), canceledDirectory)
	require.NoError(t, err)
	waitMarker(t, canceledDirectory, "started")
	require.NoError(t, secondFlow.Cancel(t.Context(), canceledID))
	_, err = firstFlow.Get(t.Context(), canceledID)
	require.ErrorIs(t, err, cord.ErrRunCanceled)
}

func TestWorkflowAsyncResultsSurviveRestart(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	firstRuntime := newRuntimeForDB(t, database)
	completedFlow := firstRuntime.From("async-restart-completed", addOne)
	failedFlow := firstRuntime.From("async-restart-failed", failAsyncPermanently)

	completedID, err := completedFlow.Submit(t.Context(), 40, "restart-key")
	require.NoError(t, err)
	completed, err := completedFlow.Get(t.Context(), completedID)
	require.NoError(t, err)
	assert.Equal(t, 41, completed)

	failedID, err := failedFlow.Submit(t.Context(), 7)
	require.NoError(t, err)
	_, err = failedFlow.Get(t.Context(), failedID)
	require.EqualError(t, err, "async step: "+errAsyncPersisted.Error())
	require.NoError(t, firstRuntime.Close())

	secondRuntime := newRuntimeForDB(t, database)
	restartedCompleted := secondRuntime.From("async-restart-completed", addOne)
	restartedFailed := secondRuntime.From("async-restart-failed", failAsyncPermanently)

	completed, err = restartedCompleted.Get(t.Context(), completedID)
	require.NoError(t, err)
	assert.Equal(t, 41, completed)
	attachedID, err := restartedCompleted.Submit(t.Context(), 40, "restart-key")
	require.NoError(t, err)
	assert.Equal(t, completedID, attachedID)
	require.ErrorIs(t, restartedCompleted.Cancel(t.Context(), completedID), cord.ErrRunFinished)

	_, err = restartedFailed.Get(t.Context(), failedID)
	require.EqualError(t, err, "async step: "+errAsyncPersisted.Error())
	assert.ErrorIs(t, restartedFailed.Cancel(t.Context(), failedID), cord.ErrRunFinished)
}
