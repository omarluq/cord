package sqlite_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_ClaimReadyNodeForFunctionsMatchesExactRegistration(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "registered-claim")
	requireCreateRun(t.Context(), t, store, &plan)

	claim, claimed, err := store.ClaimReadyNodeForFunctions(t.Context(), "worker", time.Minute, nil)
	require.NoError(t, err)
	assert.False(t, claimed)
	assert.Nil(t, claim)

	wrongRegistration := []storage.FunctionRegistration{{Key: plan.Nodes[0].FunctionKey, Signature: "wrong-signature"}}
	claim, claimed, err = store.ClaimReadyNodeForFunctions(t.Context(), "worker", time.Minute, wrongRegistration)
	require.NoError(t, err)
	assert.False(t, claimed)
	assert.Nil(t, claim)

	registration := []storage.FunctionRegistration{
		{Key: plan.Nodes[0].FunctionKey, Signature: plan.Nodes[0].SignatureHash},
	}
	claim, claimed, err = store.ClaimReadyNodeForFunctions(t.Context(), "worker", time.Minute, registration)
	require.NoError(t, err)
	require.True(t, claimed)
	assert.Equal(t, plan.Nodes[0].FunctionKey, claim.FunctionKey)
	assert.Equal(t, plan.Nodes[0].SignatureHash, claim.SignatureHash)
}

func TestStore_LoadNodeInputsUsesRootThenOrderedParentOutputs(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	now := time.Now().UTC()
	plan := validPlan(now, "node-inputs")
	plan.Nodes = []storage.Node{
		plan.Nodes[0],
		newNode(plan.Run.ID, "lint", "example.com/workflow.Lint", "lint-signature", storage.NodeReady, now, 0),
		newNode(
			plan.Run.ID, terminalNode, "example.com/workflow.Publish", "publish-signature", storage.NodePending, now, 2,
		),
	}
	plan.Edges = []storage.Edge{
		{RunID: plan.Run.ID, Parent: compileNode, Child: terminalNode, ParentOrder: 1},
		{RunID: plan.Run.ID, Parent: "lint", Child: terminalNode, ParentOrder: 0},
	}
	requireCreateRun(t.Context(), t, store, &plan)

	inputs, err := store.LoadNodeInputs(t.Context(), plan.Run.ID, plan.Nodes[0].ID)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	assert.JSONEq(t, string(plan.Run.Input), string(inputs[0]))

	outputs := map[storage.NodeID][]byte{
		compileNode: []byte(`"compile-output"`),
		"lint":      []byte(`"lint-output"`),
	}
	for range outputs {
		claim := claimNode(t, store)
		require.True(t, completeNode(t, store, claim, outputs[claim.NodeID]))
	}

	inputs, err = store.LoadNodeInputs(t.Context(), plan.Run.ID, "publish")
	require.NoError(t, err)
	require.Len(t, inputs, 2)
	assert.JSONEq(t, `"lint-output"`, string(inputs[0]))
	assert.JSONEq(t, `"compile-output"`, string(inputs[1]))
}

func TestStore_RetryPromotionAndRunResult(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "retry-result")
	requireCreateRun(t.Context(), t, store, &plan)
	claim := claimNode(t, store)

	accepted, err := store.RetryNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`{"message":"again"}`), time.Hour,
	)
	require.NoError(t, err)
	require.True(t, accepted)
	assertNodeState(t, database, claim.RunID, claim.NodeID, storage.NodeRetryWait, 0)

	promoted, err := store.PromoteRetries(t.Context())
	require.NoError(t, err)
	assert.Zero(t, promoted)
	_, err = database.ExecContext(t.Context(), "UPDATE cord_nodes SET available_at = datetime('now', '-1 second')")
	require.NoError(t, err)
	promoted, err = store.PromoteRetries(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 1, promoted)

	second := claimNode(t, store)
	require.True(t, completeNode(t, store, second, []byte(`"compiled"`)))
	terminal := claimNode(t, store)
	require.True(t, completeNode(t, store, terminal, []byte(`"done"`)))

	result, err := store.GetRunResult(t.Context(), plan.Run.ID)
	require.NoError(t, err)
	assert.Equal(t, plan.Run.WorkflowName, result.WorkflowName)
	assert.Equal(t, plan.Run.DefinitionHash, result.DefinitionHash)
	assert.Equal(t, plan.Nodes[1].SignatureHash, result.TerminalSignatureHash)
	assert.Equal(t, plan.Run.MaxAttempts, result.MaxAttempts)
	assert.Equal(t, plan.Run.RetryBaseDelay, result.RetryBaseDelay)
	assert.Equal(t, plan.Run.RetryMaxDelay, result.RetryMaxDelay)
	assert.Equal(t, plan.Run.RetryPolicyVersion, result.RetryPolicyVersion)
	assert.Equal(t, storage.RunCompleted, result.Status)
	assert.JSONEq(t, `"done"`, string(result.Output))
	assert.Nil(t, result.Error)

	_, err = store.GetRunResult(t.Context(), "absent")
	require.ErrorIs(t, err, storage.ErrRunNotFound)
	require.ErrorContains(t, err, "read run result")
}

func completeNode(t *testing.T, store *sqlite.Store, claim *storage.Claim, output []byte) bool {
	t.Helper()
	accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, output)
	require.NoError(t, err)

	return accepted
}
