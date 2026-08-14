package cord_test

import (
	"context"
	"database/sql"
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

func TestScheduler_ExhaustsRetryPolicyAndCountsClaims(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)
	require.NoError(t, runtime.SetRetryPolicy(cord.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
	}))

	_, err := runtime.From("test-workflow", alwaysFails).Run(t.Context(), 4)
	require.ErrorContains(t, err, "still failing")
	assertNodeAttempt(t, database, 3)
}

func TestScheduler_PermanentErrorSkipsRetries(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)

	_, err := runtime.From("test-workflow", failsPermanently).Run(t.Context(), 4)
	require.EqualError(t, err, "marked failure: step failed")
	assertNodeAttempt(t, database, 1)
}

func TestScheduler_RetrySurvivesRuntimeRestart(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	first, err := cord.New(database)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	require.NoError(t, first.SetRetryPolicy(cord.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Hour,
		MaxDelay:    time.Hour,
	}))

	result := make(chan error, 1)

	go func() {
		_, runErr := first.From("test-workflow", alwaysFails).Run(t.Context(), 1)
		result <- runErr
	}()

	require.Eventually(t, func() bool {
		var status string

		queryErr := database.QueryRowContext(t.Context(), "SELECT status FROM cord_nodes").Scan(&status)

		return queryErr == nil && status == retryWaitStatus
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, first.Close())
	require.ErrorContains(t, <-result, "runtime closed")
	_, err = database.ExecContext(t.Context(), "UPDATE cord_nodes SET available_at = datetime('now', '-1 second')")
	require.NoError(t, err)

	second, err := cord.New(database)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.SetRetryPolicy(cord.RetryPolicy{
		MaxAttempts: 2,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
	}))
	second.From("test-workflow", alwaysFails)

	require.Eventually(t, func() bool {
		var (
			status  string
			attempt int
		)

		queryErr := database.QueryRowContext(
			t.Context(),
			"SELECT status, attempt FROM cord_nodes",
		).Scan(&status, &attempt)

		return queryErr == nil && status == retryWaitStatus && attempt == 2
	}, 5*time.Second, 10*time.Millisecond)
	_, err = database.ExecContext(t.Context(), "UPDATE cord_nodes SET available_at = datetime('now', '-1 second')")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var status string

		queryErr := database.QueryRowContext(t.Context(), "SELECT status FROM cord_runs").Scan(&status)

		return queryErr == nil && status == "failed"
	}, 5*time.Second, 10*time.Millisecond)
	assertNodeAttempt(t, database, 3)
}

// TestScheduler_EagerRegistrationRecoversPersistedWork verifies that persisted work
// resumes after its workflow is registered in a new runtime.
func TestScheduler_EagerRegistrationRecoversPersistedWork(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	first, err := cord.NewWithOptions(database, cord.RuntimeOptions{PollInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	require.NoError(t, first.SetRetryPolicy(cord.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Hour,
		MaxDelay:    time.Hour,
	}))

	directory := t.TempDir()
	result := make(chan error, 1)

	go func() {
		_, runErr := first.From("restart-registration", succeedsWhenReleased).Run(t.Context(), directory)
		result <- runErr
	}()

	require.Eventually(t, func() bool {
		var status string

		queryErr := database.QueryRowContext(t.Context(), "SELECT status FROM cord_nodes").Scan(&status)

		return queryErr == nil && status == retryWaitStatus
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, first.Close())
	require.ErrorContains(t, <-result, "runtime closed")

	_, err = database.ExecContext(t.Context(),
		"UPDATE cord_nodes SET available_at = datetime('now', '-1 second')")
	require.NoError(t, err)

	second, err := cord.NewWithOptions(database, cord.RuntimeOptions{PollInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	require.Eventually(t, func() bool {
		var status string

		queryErr := database.QueryRowContext(t.Context(), "SELECT status FROM cord_nodes").Scan(&status)

		return queryErr == nil && status == "ready"
	}, 5*time.Second, 10*time.Millisecond)
	assert.Never(t, func() bool {
		var (
			status  string
			attempt int
		)

		queryErr := database.QueryRowContext(t.Context(),
			"SELECT status, attempt FROM cord_nodes").Scan(&status, &attempt)

		return queryErr != nil || status != "ready" || attempt != 1
	}, 200*time.Millisecond, 10*time.Millisecond)

	require.NoError(t, writeMarker(directory, "release"))
	second.From("restart-registration", succeedsWhenReleased)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var (
			status string
			output string
		)

		err := database.QueryRowContext(t.Context(),
			"SELECT status, output_payload FROM cord_runs").Scan(&status, &output)
		require.NoError(collect, err)
		assert.Equal(collect, "completed", status)
		assert.JSONEq(collect, `"recovered"`, output)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestScheduler_CallerCancellationDuringRetryWaitLeavesRunDurable(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)
	require.NoError(t, runtime.SetRetryPolicy(cord.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Hour,
		MaxDelay:    time.Hour,
	}))
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)

	go func() {
		_, err := runtime.From("test-workflow", alwaysFails).Run(ctx, 1)
		result <- err
	}()

	require.Eventually(t, func() bool {
		var status string

		err := database.QueryRowContext(t.Context(), "SELECT status FROM cord_nodes").Scan(&status)

		return err == nil && status == retryWaitStatus
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)

	var runStatus, nodeStatus string
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT status FROM cord_runs").Scan(&runStatus))
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT status FROM cord_nodes").Scan(&nodeStatus))
	assert.Equal(t, "running", runStatus)
	assert.Equal(t, retryWaitStatus, nodeStatus)
	assertNodeAttempt(t, database, 1)
}

func assertNodeAttempt(t *testing.T, database *sql.DB, expected int) {
	t.Helper()

	var attempt int
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT attempt FROM cord_nodes ORDER BY node_id LIMIT 1").Scan(&attempt))
	assert.Equal(t, expected, attempt)
}
