package cord_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func recoverExpiredPollingLease(ctx context.Context, directory string) (string, error) {
	firstMarker := filepath.Join(directory, "first-attempt")

	_, statErr := os.Stat(firstMarker)
	if os.IsNotExist(statErr) {
		if markerErr := writeMarker(directory, "first-attempt"); markerErr != nil {
			return "", markerErr
		}

		<-ctx.Done()

		return "", fmt.Errorf("first polling attempt: %w", ctx.Err())
	}

	if statErr != nil {
		return "", fmt.Errorf("inspect first polling attempt: %w", statErr)
	}

	if err := writeMarker(directory, "retried"); err != nil {
		return "", err
	}

	return "recovered", nil
}

func TestScheduler_IdlePollingRemainsBounded(t *testing.T) {
	t.Parallel()

	const pollInterval = 100 * time.Millisecond

	type schedulerError struct {
		at  time.Time
		err error
	}

	schedulerErrors := make(chan schedulerError, 4)
	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database, cord.Options{
		PollInterval: pollInterval,
		OnSchedulerError: func(err error) {
			select {
			case schedulerErrors <- schedulerError{at: time.Now(), err: err}:
			default:
			}
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	require.NoError(t, database.Close())

	var firstPoll schedulerError
	select {
	case firstPoll = <-schedulerErrors:
		require.Error(t, firstPoll.err)
	case <-time.After(5 * time.Second):
		require.Fail(t, "scheduler did not poll")
	}

	var secondPoll schedulerError
	select {
	case secondPoll = <-schedulerErrors:
		require.Error(t, secondPoll.err)
	case <-time.After(5 * time.Second):
		require.Fail(t, "scheduler did not resume polling")
	}

	assert.GreaterOrEqual(t, secondPoll.at.Sub(firstPoll.at), pollInterval/2,
		"idle scheduler polled continuously")
}

func TestWorkflow_RunLocalCompletionLatencyIsIndependentOfSchedulerPolling(t *testing.T) {
	t.Parallel()

	const schedulerPollInterval = time.Hour

	_, runtime := newRuntime(t, cord.Options{PollInterval: schedulerPollInterval})
	flow := runtime.From("local-result-notification", passThrough)

	started := time.Now()
	result, err := flow.Run(t.Context(), 42)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
	assert.Less(t, time.Since(started), 5*time.Second)
}

func TestScheduler_PromotesRetryWithinPollingLatency(t *testing.T) {
	t.Parallel()

	const pollInterval = 400 * time.Millisecond

	latencyLimit := pollingLatencyLimit(pollInterval)

	database, runtime := newRuntime(t, cord.Options{
		PollInterval:   pollInterval,
		MaxAttempts:    2,
		RetryBaseDelay: time.Hour,
		RetryMaxDelay:  time.Hour,
	})
	directory := t.TempDir()

	completed := make(chan workflowResult, 1)

	go func() {
		value, err := runtime.From("polling-retry-latency", succeedsWhenReleased).Run(t.Context(), directory)
		completed <- workflowResult{value: value, err: err}
	}()

	require.Eventually(t, func() bool {
		var status string

		err := database.QueryRowContext(t.Context(), "SELECT status FROM cord_nodes").Scan(&status)

		return err == nil && status == retryWaitStatus
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, writeMarker(directory, "release"))

	_, err := database.ExecContext(t.Context(),
		"UPDATE cord_nodes SET available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')")
	require.NoError(t, err)

	eligibleAt := time.Now()
	result := waitWorkflowResult(t, completed)
	require.NoError(t, result.err)
	assert.Equal(t, "recovered", result.value)
	assert.LessOrEqual(t, time.Since(eligibleAt), latencyLimit)
}

func TestScheduler_RecoversExpiredLeaseWithinPollingLatency(t *testing.T) {
	t.Parallel()

	const pollInterval = 400 * time.Millisecond

	latencyLimit := pollingLatencyLimit(pollInterval)

	database, runtime := newRuntime(t, cord.Options{
		Concurrency:       2,
		PollInterval:      pollInterval,
		LeaseTTL:          time.Second,
		HeartbeatInterval: 250 * time.Millisecond,
	})

	directory := t.TempDir()
	completed := make(chan workflowResult, 1)

	go func() {
		value, err := runtime.From("polling-lease-recovery", recoverExpiredPollingLease).
			Run(t.Context(), directory)
		completed <- workflowResult{value: value, err: err}
	}()

	waitMarker(t, directory, "first-attempt")

	_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
		WHERE status = 'running'`)
	require.NoError(t, err)

	expiredAt := time.Now()

	waitMarker(t, directory, "retried")
	assert.LessOrEqual(t, time.Since(expiredAt), latencyLimit)

	result := waitWorkflowResult(t, completed)
	require.NoError(t, result.err)
	assert.Equal(t, "recovered", result.value)
}

// pollingLatencyLimit permits three polling periods plus enough scheduling
// allowance for parallel, race-enabled test execution on a loaded runner.
func pollingLatencyLimit(pollInterval time.Duration) time.Duration {
	const schedulingAllowance = 2 * time.Second

	return 3*pollInterval + schedulingAllowance
}

func TestCord_CloseInterruptsLongPollingWait(t *testing.T) {
	t.Parallel()

	runtime, err := cord.New(t.Context(), openSQLite(t), cord.Options{PollInterval: time.Hour})
	require.NoError(t, err)

	closed := make(chan error, 1)
	go func() {
		closed <- runtime.Close()
	}()

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.Fail(t, "Close did not interrupt the polling wait")
	}
}
