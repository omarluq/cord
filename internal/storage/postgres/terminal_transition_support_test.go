package postgres_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func terminalRacePlan(runID storage.RunID) storage.RunPlan {
	now := time.Now().UTC().Add(-time.Second)

	return storage.RunPlan{
		Edges: []storage.Edge{{
			RunID: runID, Parent: "sibling", Child: "terminal", ParentOrder: 0,
		}},
		Run: storage.Run{
			CreatedAt: now, UpdatedAt: now, CompletedAt: nil, StartedAt: nil,
			TerminalReason: nil, TerminalRunnerID: nil,
			ID: runID, WorkflowName: string(runID), DefinitionHash: "definition",
			TerminalNodeID: "terminal", Status: storage.RunRunning,
			Input: []byte(`null`), Output: nil, Error: nil,
			MaxAttempts: 1, RetryBaseDelay: time.Millisecond,
			RetryMaxDelay: time.Second, RetryPolicyVersion: 1,
			IdempotencyKey: nil, SubmissionFingerprint: nil,
		},
		Nodes: []storage.Node{
			terminalRaceNode(runID, "terminal", "terminal-key", "terminal-signature", now, 1),
			terminalRaceNode(runID, "sibling", "sibling-key", "sibling-signature", now, 0),
		},
	}
}

func terminalRaceNode(
	runID storage.RunID,
	nodeID storage.NodeID,
	key, signature string,
	availableAt time.Time,
	remainingDeps int,
) storage.Node {
	status := storage.NodeReady
	if remainingDeps > 0 {
		status = storage.NodePending
	}

	return storage.Node{
		AvailableAt: availableAt, CompletedAt: nil, StartedAt: nil, StateChangedAt: nil,
		LastStartedAt: nil, LastRunnerID: nil, TerminalReason: nil,
		SignatureHash: signature, RunID: runID, ID: nodeID, FunctionKey: key,
		Status: status, Lease: storage.Lease{}, Error: nil, Output: nil,
		RemainingDeps: remainingDeps, Attempt: 0,
	}
}

func claimPostgresNode(
	t *testing.T,
	store *postgres.Store,
	owner, key, signature string,
) *storage.Claim {
	t.Helper()

	claim, claimed, err := store.ClaimReadyNodeForFunctions(
		t.Context(), owner, time.Minute,
		[]storage.FunctionRegistration{{Key: key, Signature: signature}},
	)
	require.NoError(t, err)
	require.True(t, claimed)

	return claim
}

func assertTerminalRaceState(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	completionAllowed storage.RunStatus,
) {
	t.Helper()

	var runStatus storage.RunStatus
	require.NoError(t, database.QueryRowContext(
		t.Context(), `SELECT status FROM cord_runs WHERE id = $1`, runID,
	).Scan(&runStatus))

	if completionAllowed == storage.RunCompleted {
		assert.Contains(t, []storage.RunStatus{storage.RunCompleted, storage.RunFailed}, runStatus)
	} else {
		assert.Equal(t, storage.RunFailed, runStatus)
	}

	rows, err := database.QueryContext(
		t.Context(), `SELECT status FROM cord_nodes WHERE run_id = $1 ORDER BY node_id`, runID,
	)
	require.NoError(t, err)

	defer func() { require.NoError(t, rows.Close()) }()

	statuses := make([]storage.NodeStatus, 0, 2)

	for rows.Next() {
		var status storage.NodeStatus
		require.NoError(t, rows.Scan(&status))
		statuses = append(statuses, status)
	}

	require.NoError(t, rows.Err())
	require.Len(t, statuses, 2)

	if runStatus == storage.RunCompleted {
		assert.ElementsMatch(t, []storage.NodeStatus{storage.NodeCompleted, storage.NodeRunning}, statuses)

		var unfinishedReason sql.NullString
		require.NoError(t, database.QueryRowContext(
			t.Context(), `SELECT terminal_reason FROM cord_nodes
				WHERE run_id = $1 AND status = $2`, runID, storage.NodeRunning,
		).Scan(&unfinishedReason))
		assert.False(t, unfinishedReason.Valid)
	} else {
		assert.ElementsMatch(t, []storage.NodeStatus{storage.NodeCanceled, storage.NodeFailed}, statuses)
	}
}
