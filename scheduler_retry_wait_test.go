package cord_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduler_CallerCancellationDuringRetryWaitLeavesRunDurable(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t, cord.Options{
		MaxAttempts: 3, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour,
	})
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

func assertTerminalReasons(t *testing.T, database *sql.DB, expected string) {
	t.Helper()

	var runReason, nodeReason string
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT terminal_reason FROM cord_runs").Scan(&runReason))
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT terminal_reason FROM cord_nodes").Scan(&nodeReason))
	assert.Equal(t, expected, runReason)
	assert.Equal(t, expected, nodeReason)
}
