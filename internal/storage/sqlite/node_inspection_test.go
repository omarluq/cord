package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStore_ListRunNodesKeysetFiltersAndBounds(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	plan := inspectionLinearPlan("node-page", 4)
	requireCreateRun(t.Context(), t, store, &plan)

	page, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: "", PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 2)
	assert.Equal(t, storage.NodeID("node-00"), page.Nodes[0].NodeID)
	assert.Equal(t, "node-01", page.ContinuationToken)

	next, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: page.ContinuationToken, PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, next.Nodes, 2)
	assert.Equal(t, storage.NodeID("node-02"), next.Nodes[0].NodeID)
	assert.Empty(t, next.ContinuationToken)

	state := storage.NodePending
	filtered, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: &state, Reason: nil, ContinuationToken: "", PageSize: 0,
	})
	require.NoError(t, err)
	require.Len(t, filtered.Nodes, 3)

	for _, node := range filtered.Nodes {
		assert.Equal(t, storage.NodePending, node.State)
	}

	_, err = store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: "", PageSize: storage.MaxNodePageSize + 1,
	})
	require.Error(t, err)
	_, err = store.ListRunNodes(t.Context(), "missing", storage.NodeQuery{})
	assert.ErrorIs(t, err, storage.ErrRunNotFound)
}

func TestStore_ListRunNodesValidatesCurrentNodeStartMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                               string
		clearFirst, clearLast, clearRunner bool
	}{
		{name: "missing first start", clearFirst: true, clearLast: false, clearRunner: false},
		{name: "missing last start", clearFirst: false, clearLast: true, clearRunner: false},
		{name: "missing runner", clearFirst: false, clearLast: false, clearRunner: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("start-metadata-"+testCase.name))
			requireCreateRun(t.Context(), t, store, &plan)

			_, claimed, err := store.ClaimReadyNode(t.Context(), "worker", time.Minute)
			require.NoError(t, err)
			require.True(t, claimed)

			_, err = database.ExecContext(t.Context(), `UPDATE cord_nodes
				SET status = 'retry_wait', lease_owner = NULL, lease_expires_at = NULL,
					started_at = CASE WHEN ? THEN NULL ELSE started_at END,
					last_started_at = CASE WHEN ? THEN NULL ELSE last_started_at END,
					last_runner_id = CASE WHEN ? THEN NULL ELSE last_runner_id END
				WHERE run_id = ? AND node_id = ?`,
				testCase.clearFirst, testCase.clearLast, testCase.clearRunner,
				plan.Run.ID, plan.Nodes[0].ID)
			require.NoError(t, err)

			_, err = store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
			assert.ErrorIs(t, err, storage.ErrRunIncompatible)
		})
	}
}
