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

type leaseTestInput struct {
	Directory string
}

type workflowResult struct {
	err   error
	value string
}

func probedLeaseStep(ctx context.Context, input leaseTestInput) (string, error) {
	first := filepath.Join(input.Directory, "first")

	err := os.Mkdir(first, 0o700)
	if err == nil {
		return runFirstLeaseInvocation(ctx, input.Directory)
	}

	if !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("create first-invocation marker: %w", err)
	}

	if markerErr := writeMarker(input.Directory, "second-started"); markerErr != nil {
		return "", markerErr
	}

	if waitErr := waitForMarker(ctx, input.Directory, "release-second"); waitErr != nil {
		return "", waitErr
	}

	return "winner", nil
}

func runFirstLeaseInvocation(ctx context.Context, directory string) (string, error) {
	if err := writeMarker(directory, "first-started"); err != nil {
		return "", err
	}

	select {
	case <-ctx.Done():
		if err := writeMarker(directory, "first-canceled"); err != nil {
			return "", err
		}

		// The lease-loss context is already canceled; keep the stale invocation
		// in flight until the test explicitly releases it.
		if err := waitForMarker(context.WithoutCancel(ctx), directory, "release-first"); err != nil {
			return "", err
		}

		if err := writeMarker(directory, "first-finished"); err != nil {
			return "", err
		}

		return "stale", nil
	case <-markerSignal(ctx, directory, "release-first"):
		return "winner", nil
	}
}

func writeMarker(directory, name string) error {
	if err := os.WriteFile(filepath.Join(directory, name), nil, 0o600); err != nil {
		return fmt.Errorf("write lease marker %q: %w", name, err)
	}

	return nil
}

func waitForMarker(ctx context.Context, directory, name string) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for lease marker %q: %w", name, ctx.Err())
	case <-markerSignal(ctx, directory, name):
		return nil
	}
}

func markerSignal(ctx context.Context, directory, name string) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		for {
			if _, err := os.Stat(filepath.Join(directory, name)); err == nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return done
}

func TestScheduler_HeartbeatKeepsLongRunningInvocationLeased(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	config := testConfig()
	first := newRuntimeWithConfig(t, database, config)
	second := newRuntimeWithConfig(t, database, config)
	flow := first.From("test-workflow", probedLeaseStep)
	second.From("test-workflow", probedLeaseStep)

	directory := t.TempDir()
	cleanupLeaseMarkers(t, directory)

	result := runLeaseWorkflow(t, flow, leaseTestInput{Directory: directory})

	waitMarker(t, directory, "first-started")
	initialExpiry := readLeaseExpiry(t, database)
	require.Eventually(t, func() bool {
		return readLeaseExpiry(t, database).After(initialExpiry)
	}, 5*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return readLeaseExpiry(t, database).After(time.Now().Add(config.LeaseTTL / 2))
	}, 5*time.Second, config.HeartbeatInterval)
	assertNodeLeaseState(t, database, "running", 1)
	require.NoError(t, writeMarker(directory, "release-first"))

	completed := waitWorkflowResult(t, result)
	require.NoError(t, completed.err)
	assert.Equal(t, "winner", completed.value)
}

func TestScheduler_LeaseLossCancelsOldExecutionAndPreservesNewCompletion(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	config := testConfig()
	first := newRuntimeWithConfig(t, database, config)
	second := newRuntimeWithConfig(t, database, config)
	flow := first.From("test-workflow", probedLeaseStep)
	directory := t.TempDir()
	cleanupLeaseMarkers(t, directory)

	result := runLeaseWorkflow(t, flow, leaseTestInput{Directory: directory})

	waitMarker(t, directory, "first-started")
	firstOwner := readLeaseOwner(t, database)

	second.From("test-workflow", probedLeaseStep)
	invalidateLease(t, database, firstOwner)

	waitMarker(t, directory, "first-canceled")
	waitMarker(t, directory, "second-started")
	assertNodeLeaseState(t, database, "running", 2)
	assert.NotEqual(t, firstOwner, readLeaseOwner(t, database))
	require.NoError(t, writeMarker(directory, "release-second"))

	completed := waitWorkflowResult(t, result)
	require.NoError(t, completed.err)
	assert.Equal(t, "winner", completed.value)
	require.NoError(t, writeMarker(directory, "release-first"))
	waitMarker(t, directory, "first-finished")

	var output string
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT output_payload FROM cord_nodes").Scan(&output))
	assert.JSONEq(t, `"winner"`, output)
}

func testConfig() cord.Config {
	return cord.Config{
		Concurrency:       1,
		PollInterval:      20 * time.Millisecond,
		LeaseTTL:          2 * time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
	}
}

func newRuntimeWithConfig(t *testing.T, database *sql.DB, config cord.Config) *cord.Cord {
	t.Helper()

	runtime, err := cord.New(database, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	return runtime
}

func runLeaseWorkflow(
	t *testing.T,
	flow cord.Workflow[leaseTestInput, string],
	input leaseTestInput,
) <-chan workflowResult {
	t.Helper()

	result := make(chan workflowResult, 1)

	go func() {
		value, err := flow.Run(t.Context(), input)
		result <- workflowResult{err: err, value: value}
	}()

	return result
}

func cleanupLeaseMarkers(t *testing.T, directory string) {
	t.Helper()

	t.Cleanup(func() {
		for _, name := range []string{"release-first", "release-second"} {
			if err := writeMarker(directory, name); err != nil {
				t.Logf("release lease marker during cleanup: %v", err)
			}
		}
	})
}

func waitMarker(t *testing.T, directory, name string) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(directory, name))

		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}

func invalidateLease(t *testing.T, database *sql.DB, owner string) {
	t.Helper()

	result, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second'),
			lease_generation = lease_generation + 1
		WHERE status = 'running' AND lease_owner = ?`, owner)
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
}

func readLeaseExpiry(t *testing.T, database *sql.DB) time.Time {
	t.Helper()

	var millis int64
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT
		CAST((julianday(lease_expires_at) - 2440587.5) * 86400000 AS INTEGER)
		FROM cord_nodes WHERE status = 'running' ORDER BY node_id LIMIT 1`).Scan(&millis))

	return time.UnixMilli(millis).UTC()
}

func readLeaseOwner(t *testing.T, database *sql.DB) string {
	t.Helper()

	var owner sql.NullString
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT lease_owner FROM cord_nodes WHERE status = 'running' ORDER BY node_id LIMIT 1").Scan(&owner))
	require.True(t, owner.Valid)

	return owner.String
}

func assertNodeLeaseState(t *testing.T, database *sql.DB, status string, attempt int) {
	t.Helper()

	var (
		actualStatus  string
		actualAttempt int
	)
	require.NoError(t, database.QueryRowContext(t.Context(),
		"SELECT status, attempt FROM cord_nodes ORDER BY node_id LIMIT 1").Scan(&actualStatus, &actualAttempt))
	assert.Equal(t, status, actualStatus)
	assert.Equal(t, attempt, actualAttempt)
}

func waitWorkflowResult(t *testing.T, result <-chan workflowResult) workflowResult {
	t.Helper()

	select {
	case completed := <-result:
		return completed
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for workflow result")

		return workflowResult{}
	}
}
