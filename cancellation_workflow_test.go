package cord_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	runtime, flow, runID := submitBlockedQueuedRun(t, "async-cancel-run-id-only")
	invalid := runtime.From("", completeAfterRelease)

	require.NoError(t, invalid.Cancel(t.Context(), runID))
	_, err := flow.Get(t.Context(), runID)
	require.ErrorIs(t, err, cord.ErrRunCanceled)
}

func TestWorkflowCancelImmediatelyProducesAuthoritativeSnapshot(t *testing.T) {
	t.Parallel()

	runtime, flow, runID := submitBlockedQueuedRun(t, "async-cancel-inspect")
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

			runtime, flow, runID := submitBlockedQueuedRun(
				t,
				fmt.Sprintf("async-cancel-callers-%d", callers),
			)

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
