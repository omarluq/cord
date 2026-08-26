package cord_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
