package cord_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkflow_LogicalIDsRemainCompatible verifies stable logical IDs and definition hashes.
func TestWorkflow_LogicalIDsRemainCompatible(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)
	root := runtime.From("logical-id-golden", passThrough)
	_ = root.Then(timesTwo) // Unreachable siblings must not affect reachable occurrences.
	left := root.Then(timesTwo)
	right := root.Then(timesTwo)

	result, err := cord.Join(left, right).Then(subtract).Run(t.Context(), 3)
	require.NoError(t, err)
	assert.Zero(t, result)

	rows, err := database.QueryContext(t.Context(), `
		SELECT n.node_id, n.function_key, COALESCE(e.parent_order, -1)
		FROM cord_nodes n
		LEFT JOIN cord_edges e ON e.run_id = n.run_id AND e.child_node_id = n.node_id
		ORDER BY n.function_key, e.parent_order, n.node_id`)
	require.NoError(t, err)

	defer func() { require.NoError(t, rows.Close()) }()

	type persistedNode struct {
		id          string
		functionKey string
		parentOrder int
	}

	var persisted []persistedNode

	for rows.Next() {
		var current persistedNode
		require.NoError(t, rows.Scan(&current.id, &current.functionKey, &current.parentOrder))
		persisted = append(persisted, current)
	}

	require.NoError(t, rows.Err())

	assert.Equal(t, []persistedNode{
		{
			id:          "7ec7945a1d72dcbe1d44f5b72959394b0fd92ad2cd63c0df6d8fbf37b897528c",
			functionKey: "github.com/omarluq/cord_test.passThrough", parentOrder: -1,
		},
		{
			id:          "53e504a3091423e8bda834b085afb0da52e1f73ec1e264524c462c2269f98046",
			functionKey: "github.com/omarluq/cord_test.subtract", parentOrder: 0,
		},
		{
			id:          "53e504a3091423e8bda834b085afb0da52e1f73ec1e264524c462c2269f98046",
			functionKey: "github.com/omarluq/cord_test.subtract", parentOrder: 1,
		},
		{
			id:          "a58dd399a6b8e2153df796055e781b2c720b56e05f343acb2bb35fb25d5364b0",
			functionKey: "github.com/omarluq/cord_test.timesTwo", parentOrder: 0,
		},
		{
			id:          "f1e66e8cc7b6977db5067f077df40824d06401a9ec9e4ea6e7545753696bc31a",
			functionKey: "github.com/omarluq/cord_test.timesTwo", parentOrder: 0,
		},
	}, persisted)

	var definitionHash string
	require.NoError(t, database.QueryRowContext(
		t.Context(), "SELECT definition_hash FROM cord_runs",
	).Scan(&definitionHash))
	assert.Equal(t, "844aa38288a392d43d41a13c5af217291ca9541a93c38d9e0b4a0d483e9b9660", definitionHash)
}

// TestWorkflow_PersistedPlanFromBeforeLogicalIDRefactorRemainsExecutable verifies persisted-plan compatibility.
func TestWorkflow_PersistedPlanFromBeforeLogicalIDRefactorRemainsExecutable(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	store, err := sqlite.New(database)
	require.NoError(t, err)

	inputFingerprint, err := serialization.NewJSONCodec[int]()
	require.NoError(t, err)
	fingerprint, err := inputFingerprint.TypeFingerprint()
	require.NoError(t, err)

	signature := serialization.SignatureFingerprint([]string{fingerprint}, fingerprint)

	now := time.Now().UTC()
	runID := storage.RunID("01900000-0000-7000-8000-000000000009")
	nodeID := storage.NodeID("6c5b0f920fab45ba04a898c2184e4f6596f4c41149f1977ada36064320aad502")
	plan := storage.RunPlan{
		Run: storage.Run{
			CreatedAt: now, UpdatedAt: now, CompletedAt: nil, StartedAt: nil,
			TerminalReason: nil, TerminalRunnerID: nil,
			ID: runID, WorkflowName: "persisted-plan-golden",
			DefinitionHash: "persisted-definition-hash", TerminalNodeID: nodeID,
			Status: storage.RunRunning, Input: []byte("7"), Output: nil, Error: nil,
			MaxAttempts: 3, RetryBaseDelay: 500 * time.Millisecond, RetryMaxDelay: 30 * time.Second,
			RetryPolicyVersion:    1,
			IdempotencyKey:        nil,
			SubmissionFingerprint: nil,
		},
		Nodes: []storage.Node{{
			AvailableAt: now, CompletedAt: nil, StartedAt: nil,
			StateChangedAt: nil, LastStartedAt: nil,
			LastRunnerID: nil, TerminalReason: nil,
			FunctionKey: "github.com/omarluq/cord_test.passThrough", RunID: runID, ID: nodeID,
			SignatureHash: signature, Status: storage.NodeReady, Error: nil, Output: nil,
			Lease: storage.Lease{}, RemainingDeps: 0, Attempt: 0,
		}},
		Edges: []storage.Edge{},
	}
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	runtime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })
	runtime.From("persisted-plan-golden", passThrough)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		result, resultErr := store.GetRunResult(t.Context(), runID)
		if !assert.NoError(collect, resultErr) {
			return
		}

		assert.Equal(collect, storage.RunCompleted, result.Status)
		assert.JSONEq(collect, "7", string(result.Output))
	}, 5*time.Second, 10*time.Millisecond)
}

func TestWorkflow_RunExcludesUnreachableBranches(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)
	root := runtime.From("test-workflow", passThrough)
	selected := root.Then(addOne)
	_ = root.Then(timesTwo)

	result, err := selected.Run(t.Context(), 4)
	require.NoError(t, err)
	assert.Equal(t, 5, result)

	var nodeCount int
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_nodes").Scan(&nodeCount))
	assert.Equal(t, 2, nodeCount)
}
