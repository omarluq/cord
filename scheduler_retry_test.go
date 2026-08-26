package cord_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/require"
)

func TestScheduler_ExhaustsRetryPolicyAndCountsClaims(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t, cord.Options{
		MaxAttempts: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
	})

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
	assertTerminalReasons(t, database, "failure_non_retryable")
}

func TestScheduler_ExhaustedFailureRecordsReason(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t, cord.Options{
		MaxAttempts: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
	})

	_, err := runtime.From("test-workflow", alwaysFails).Run(t.Context(), 4)
	require.EqualError(t, err, "still failing")
	assertTerminalReasons(t, database, "failure_attempts_exhausted")
}

func TestScheduler_RetrySurvivesRuntimeRestart(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	first, err := cord.New(t.Context(), database, cord.Options{
		MaxAttempts: 3, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })

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

	second, err := cord.New(t.Context(), database, cord.Options{
		MaxAttempts: 2, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
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
