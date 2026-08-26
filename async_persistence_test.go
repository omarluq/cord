package cord_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
