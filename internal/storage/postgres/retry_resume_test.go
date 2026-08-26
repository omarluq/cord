package postgres_test

import (
	"os"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReopenRegistersAndResumesPersistedRetry(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	options := cord.Options{
		PollInterval:   time.Millisecond,
		MaxAttempts:    3,
		RetryBaseDelay: time.Hour,
		RetryMaxDelay:  time.Hour,
	}
	first, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)

	marker := t.TempDir() + "/resume-ready"
	done := make(chan error, 1)

	go func() {
		_, runErr := first.From("postgres-reopen", postgresRetryUntilFileExists).Run(t.Context(), marker)
		done <- runErr
	}()

	const nodeStatusQuery = `SELECT n.status FROM cord_nodes n JOIN cord_runs r ON r.id=n.run_id
		WHERE r.workflow_name=$1`

	require.Eventually(t, func() bool {
		var status string

		queryErr := database.QueryRowContext(
			t.Context(),
			nodeStatusQuery,
			"postgres-reopen",
		).Scan(&status)

		return queryErr == nil && status == "retry_wait"
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, first.Close())
	require.ErrorContains(t, <-done, "runtime closed")

	require.NoError(t, os.WriteFile(marker, []byte("ready"), 0o600))

	const promoteQuery = `UPDATE cord_nodes SET available_at=clock_timestamp()-interval '1 second'
		WHERE status='retry_wait'`

	_, err = database.ExecContext(t.Context(), promoteQuery)
	require.NoError(t, err)
	second, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })
	second.From("postgres-reopen", postgresRetryUntilFileExists)

	const runStatusQuery = `SELECT status FROM cord_runs WHERE workflow_name=$1`

	require.Eventually(t, func() bool {
		var status string

		queryErr := database.QueryRowContext(
			t.Context(),
			runStatusQuery,
			"postgres-reopen",
		).Scan(&status)

		return queryErr == nil && status == "completed"
	}, 10*time.Second, 10*time.Millisecond)
}
