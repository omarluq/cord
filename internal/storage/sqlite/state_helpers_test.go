package sqlite_test

import (
	"database/sql"
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func claimNode(t *testing.T, store *sqlite.Store) *storage.Claim {
	t.Helper()

	claim, claimed, err := store.ClaimReadyNode(t.Context(), "worker", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotNil(t, claim)

	return claim
}

func assertNodeState(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	nodeID storage.NodeID,
	status storage.NodeStatus,
	remainingDeps int,
) {
	t.Helper()

	var (
		actualStatus    storage.NodeStatus
		actualRemaining int
	)

	err := database.QueryRowContext(t.Context(), `SELECT status, remaining_deps FROM cord_nodes
		WHERE run_id = ? AND node_id = ?`, runID, nodeID).Scan(&actualStatus, &actualRemaining)
	require.NoError(t, err)
	assert.Equal(t, status, actualStatus)
	assert.Equal(t, remainingDeps, actualRemaining)
}

func assertRunState(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	status storage.RunStatus,
	output []byte,
) {
	t.Helper()

	var (
		actualStatus storage.RunStatus
		actualOutput []byte
	)

	err := database.QueryRowContext(t.Context(), `SELECT status, output_payload FROM cord_runs WHERE id = ?`, runID).
		Scan(&actualStatus, &actualOutput)
	require.NoError(t, err)
	assert.Equal(t, status, actualStatus)
	assert.Equal(t, output, actualOutput)
}

func assertRunFailure(t *testing.T, database *sql.DB, runID storage.RunID, failure []byte) {
	t.Helper()

	var (
		status storage.RunStatus
		actual []byte
	)

	err := database.QueryRowContext(t.Context(), `SELECT status, error_payload FROM cord_runs WHERE id = ?`, runID).
		Scan(&status, &actual)
	require.NoError(t, err)
	assert.Equal(t, storage.RunFailed, status)
	assert.Equal(t, failure, actual)
}

func assertNodeReason(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	nodeID storage.NodeID,
	reason storage.TerminalReason,
) {
	t.Helper()

	var actual storage.TerminalReason

	err := database.QueryRowContext(t.Context(), `SELECT terminal_reason FROM cord_nodes
		WHERE run_id = ? AND node_id = ?`, runID, nodeID).Scan(&actual)
	require.NoError(t, err)
	assert.Equal(t, reason, actual)
}

func assertRunReason(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	reason storage.TerminalReason,
) {
	t.Helper()

	var actual storage.TerminalReason

	err := database.QueryRowContext(t.Context(), `SELECT terminal_reason FROM cord_runs WHERE id = ?`, runID).
		Scan(&actual)
	require.NoError(t, err)
	assert.Equal(t, reason, actual)
}

func openStoreDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, database.Close()) })

	return database
}
