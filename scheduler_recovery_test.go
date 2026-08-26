package cord_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScheduler_EagerRegistrationRecoversPersistedWork verifies that persisted work
// resumes after its workflow is registered in a new runtime.
func TestScheduler_EagerRegistrationRecoversPersistedWork(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	first, err := cord.New(t.Context(), database, cord.Options{
		PollInterval: 10 * time.Millisecond,
		MaxAttempts:  3, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })

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

	second, err := cord.New(t.Context(), database, cord.Options{PollInterval: 10 * time.Millisecond})
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
		if !assert.NoError(collect, err) {
			return
		}

		assert.Equal(collect, "completed", status)
		assert.JSONEq(collect, `"recovered"`, output)
	}, 5*time.Second, 10*time.Millisecond)
}
