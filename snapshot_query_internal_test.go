package cord

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListRunNodesConvertsAndResumesPortablePage(t *testing.T) {
	t.Parallel()

	now := time.Now()
	store := &snapshotStore{nodePage: storage.NodePage{
		Nodes: []storage.NodeReport{{
			EligibleAt: now, FirstStartedAt: nil, LastStartedAt: nil, StateChangedAt: nil,
			FinishedAt: nil, RunnerID: nil, CurrentLease: nil,
			RunID: "run-one", NodeID: "node-one", FunctionKey: "function",
			State: storage.NodeReady, Reason: "", Attempt: 0, MaxAttempts: 3,
		}},
		ContinuationToken: "node-one",
	}}
	runtime := openSnapshotRuntime(store)
	state := NodeStateReady

	page, err := runtime.ListRunNodes(t.Context(), "run-one", NodeQuery{State: &state, PageSize: 1})

	require.NoError(t, err)
	require.Len(t, page.Nodes, 1)
	assert.Equal(t, NodeID("node-one"), page.Nodes[0].NodeID)
	assert.Equal(t, time.UTC, page.Nodes[0].EligibleAt.Location())
	require.NotEmpty(t, page.ContinuationToken)

	otherStore := &snapshotStore{nodePage: storage.NodePage{}}
	otherRuntime := openSnapshotRuntime(otherStore)
	_, err = otherRuntime.ListRunNodes(t.Context(), "run-one", NodeQuery{
		State: &state, ContinuationToken: page.ContinuationToken,
	})

	require.NoError(t, err)
	assert.Equal(t, "node-one", otherStore.query.ContinuationToken)
	assert.Equal(t, defaultNodePageSize, otherStore.query.PageSize)
	assert.Equal(t, storage.NodeReady, *otherStore.query.State)
	assert.Nil(t, otherStore.query.Reason)
}

func TestListRunNodesRejectsInvalidQueries(t *testing.T) {
	t.Parallel()

	ready := NodeStateReady
	failed := NodeStateFailed
	mismatch, err := encodeNodePageToken(nodePageToken{
		RunID: "other", State: ready, LastNodeID: snapshotNodeID,
	})
	require.NoError(t, err)

	filterMismatch, err := encodeNodePageToken(nodePageToken{
		RunID: snapshotRunID, State: ready, LastNodeID: snapshotNodeID,
	})
	require.NoError(t, err)

	tests := []struct {
		name  string
		id    RunID
		want  string
		query NodeQuery
	}{
		{name: "negative page size", id: snapshotRunID, query: NodeQuery{PageSize: -1}, want: "page size"},
		{
			name: "oversized page", id: snapshotRunID,
			query: NodeQuery{PageSize: maxNodePageSize + 1}, want: "page size",
		},
		{
			name: "unknown state", id: snapshotRunID,
			query: NodeQuery{State: new(NodeState("unknown"))}, want: "unknown node state",
		},
		{
			name: "unknown reason", id: snapshotRunID,
			query: NodeQuery{Reason: new(TerminalReason("unknown"))}, want: "unknown terminal reason",
		},
		{
			name: "malformed token", id: snapshotRunID,
			query: NodeQuery{ContinuationToken: "%%%"}, want: "continuation token",
		},
		{
			name: "wrong run", id: snapshotRunID,
			query: NodeQuery{State: &ready, ContinuationToken: mismatch}, want: "does not match",
		},
		{
			name: "wrong filter", id: snapshotRunID,
			query: NodeQuery{State: &failed, ContinuationToken: filterMismatch}, want: "does not match",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, listErr := openSnapshotRuntime(&snapshotStore{}).ListRunNodes(t.Context(), testCase.id, testCase.query)
			assert.ErrorContains(t, listErr, testCase.want)
		})
	}
}
