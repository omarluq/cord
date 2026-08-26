package cord

import (
	"context"
	"errors"
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func TestWorkflowRunRejectsSubmissionAfterShutdownBegins(t *testing.T) {
	t.Parallel()

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		allowCreate:   make(chan struct{}),
		created:       make(chan storage.RunID, 1),
	}
	runtime := newAdmissionTestRuntime(backend)
	require.NoError(t, runtime.Close())

	_, err := runtime.From("shutdown-before-submit", admissionTestStep).Run(t.Context(), 1)
	require.EqualError(t, err, "cord: runtime closed")

	select {
	case <-backend.createStarted:
		t.Fatal("submission reached persistence after shutdown began")
	default:
	}
}

func TestWorkflowRunShutdownDuringSubmissionRejectsBeforePersistence(t *testing.T) {
	t.Parallel()

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		allowCreate:   make(chan struct{}),
		created:       make(chan storage.RunID, 1),
	}
	runtime := newAdmissionTestRuntime(backend)
	flow := runtime.From("shutdown-during-submit", admissionTestStep)

	// Hold the admission boundary after validation and plan construction are
	// available, then linearize shutdown before allowing Run to request admission.
	runtime.admissionMu.Lock()
	runDone := make(chan error, 1)

	go func() {
		_, err := flow.Run(t.Context(), 1)
		runDone <- err
	}()

	// Apply beginShutdown's admission transition while the test owns the
	// boundary, making shutdown the deterministic winner.
	runtime.acceptingRuns = false
	runtime.admissionMu.Unlock()

	require.EqualError(t, <-runDone, "cord: runtime closed")
	require.NoError(t, runtime.Close())

	select {
	case <-backend.createStarted:
		t.Fatal("submission persisted after losing admission")
	default:
	}
}

// TestWorkflowRunPersistenceWinningShutdownRaceRemainsReported verifies that a
// persisted running submission remains observable when shutdown wins afterward.
func TestWorkflowRunPersistenceWinningShutdownRaceRemainsReported(t *testing.T) {
	t.Parallel()

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		allowCreate:   make(chan struct{}),
		created:       make(chan storage.RunID, 1),
		resultRead:    make(chan struct{}),
		result:        newResultStore(testRunResult(storage.RunRunning, nil)),
	}
	runtime := newAdmissionTestRuntime(backend)
	flow := runtime.From("persist-versus-close", admissionTestStep)

	runDone := make(chan struct {
		err    error
		result int
	}, 1)
	go func() {
		result, err := flow.Run(t.Context(), 1)
		runDone <- struct {
			err    error
			result int
		}{err: err, result: result}
	}()

	<-backend.createStarted // Persistence proves submission already won admission.

	shutdownCtx, cancel := context.WithCancel(t.Context())
	cancel()

	shutdownErr := runtime.Shutdown(shutdownCtx)
	require.True(t, shutdownErr == nil || errors.Is(shutdownErr, context.Canceled))

	close(backend.allowCreate)
	createdID := <-backend.created
	require.NotEmpty(t, createdID)
	<-runtime.ctx.Done()
	<-backend.resultRead

	backend.result.set(testRunResult(storage.RunCompleted, storage.EncodedPayload("2")))
	runtime.notifyCompletion(createdID)

	outcome := <-runDone
	require.NoError(t, outcome.err)
	require.Equal(t, 2, outcome.result)
	require.NoError(t, runtime.Close())
}
