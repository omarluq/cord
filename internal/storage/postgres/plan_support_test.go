package postgres_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
)

type postgresState struct {
	nodeStatus, runStatus                    string
	leaseOwner, leaseExpiry, output, failure sql.NullString
	generation, attempt                      int64
}

func postgresDurableState(t *testing.T, database *sql.DB, runID storage.RunID, nodeID storage.NodeID) postgresState {
	t.Helper()

	var state postgresState

	err := database.QueryRowContext(t.Context(), `SELECT n.status, r.status, n.lease_owner,
		n.lease_expires_at::text, encode(n.output_payload, 'hex'), encode(n.error_payload, 'hex'),
		n.lease_generation, n.attempt FROM cord_nodes n JOIN cord_runs r ON r.id=n.run_id
		WHERE n.run_id=$1 AND n.node_id=$2`, runID, nodeID).Scan(
		&state.nodeStatus, &state.runStatus, &state.leaseOwner, &state.leaseExpiry,
		&state.output, &state.failure, &state.generation, &state.attempt)
	require.NoError(t, err)

	return state
}

const postgresTestNode storage.NodeID = "node"

func postgresReadyPlan(runID storage.RunID, availableAt time.Time) storage.RunPlan {
	return storage.RunPlan{
		Run: storage.Run{
			CreatedAt: availableAt, UpdatedAt: availableAt, CompletedAt: nil, StartedAt: nil,
			TerminalReason: nil, TerminalRunnerID: nil, ID: runID,
			WorkflowName: "postgres-concurrency", DefinitionHash: "definition", TerminalNodeID: postgresTestNode,
			Status: storage.RunRunning, Input: []byte("input"), Output: nil, Error: nil, MaxAttempts: 3,
			RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Second, RetryPolicyVersion: 1,
			IdempotencyKey: nil, SubmissionFingerprint: nil,
		},
		Nodes: []storage.Node{{
			AvailableAt: availableAt, CompletedAt: nil, StartedAt: nil, StateChangedAt: nil,
			LastStartedAt: nil, LastRunnerID: nil, TerminalReason: nil,
			SignatureHash: "signature",
			RunID:         runID, ID: postgresTestNode, FunctionKey: "postgres.test", Status: storage.NodeReady,
			Lease: storage.Lease{}, Error: nil, Output: nil, RemainingDeps: 0, Attempt: 0,
		}},
		Edges: nil,
	}
}

func postgresRegistrations() []storage.FunctionRegistration {
	return []storage.FunctionRegistration{{Key: "postgres.test", Signature: "signature"}}
}
