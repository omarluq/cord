package cord

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite" // Register the SQLite driver used by these tests.
)

func cancellationEndToEndStep(ctx context.Context, directory string) (string, error) {
	if err := os.WriteFile(filepath.Join(directory, "started"), []byte("started"), 0o600); err != nil {
		return "", fmt.Errorf("write cancellation start marker: %w", err)
	}

	<-ctx.Done()
	cancellationErr := ctx.Err()

	if err := os.WriteFile(filepath.Join(directory, "canceled"), []byte("canceled"), 0o600); err != nil {
		return "", errors.Join(cancellationErr, fmt.Errorf("write cancellation marker: %w", err))
	}

	return directory, fmt.Errorf("canceled test attempt: %w", cancellationErr)
}

type acknowledgedCancellationErrorBackend struct {
	storage.Backend
	err error
}

func (backend *acknowledgedCancellationErrorBackend) CancelRun(
	ctx context.Context,
	runID storage.RunID,
) (storage.CancellationOutcome, error) {
	outcome, err := backend.Backend.CancelRun(ctx, runID)
	if err != nil {
		return outcome, fmt.Errorf("delegate cancellation: %w", err)
	}

	return outcome, backend.err
}

func TestWorkflowCancelReconcilesCommittedCancellationEndToEnd(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "cancel.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	require.NoError(t, sqlite.Migrate(t.Context(), database))

	store, err := sqlite.New(database)
	require.NoError(t, err)

	ackErr := errors.New("cancellation acknowledgement lost")
	backend := &acknowledgedCancellationErrorBackend{Backend: store, err: ackErr}
	runtime := newCordWithSettings(backend, "cancellation-test", schedulerSettings{
		concurrency: 1, pollInterval: time.Hour, leaseTTL: defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval, retry: defaultRetryPolicy(),
	})

	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	directory := t.TempDir()
	flow := runtime.From("cancel-acknowledgement-error", cancellationEndToEndStep)
	runID, err := flow.Submit(t.Context(), directory)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(directory, "started"))

		return statErr == nil
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, flow.Cancel(t.Context(), runID))
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(directory, "canceled"))

		return statErr == nil
	}, 5*time.Second, time.Millisecond)
	_, err = flow.Get(t.Context(), runID)
	require.ErrorIs(t, err, ErrRunCanceled)

	report, err := runtime.InspectRun(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, RunStateCanceled, report.State)
	assert.Equal(t, ReasonCanceledByRequest, report.Reason)
}

func cancellationRuntime(backend storage.Backend) *Cord {
	return &Cord{
		store:             backend,
		completionWaiters: make(map[storage.RunID]*completionPoll),
		activeAttempts:    make(map[storage.RunID]map[activeAttemptKey]*activeAttempt),
	}
}
