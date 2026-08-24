package cord

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	snapshotRunID  = "run"
	snapshotNodeID = "node"
)

type snapshotStore struct {
	storage.Backend
	runErr    error
	nodeErr   error
	nodePage  storage.NodePage
	query     storage.NodeQuery
	runReport storage.RunReport
}

func (store *snapshotStore) InspectRun(context.Context, storage.RunID) (storage.RunReport, error) {
	return store.runReport, store.runErr
}

func (store *snapshotStore) ListRunNodes(
	_ context.Context,
	_ storage.RunID,
	query storage.NodeQuery,
) (storage.NodePage, error) {
	store.query = query

	return store.nodePage, store.nodeErr
}

func TestRunnerID(t *testing.T) {
	t.Parallel()

	first := &Cord{owner: "runner-one"}
	second := &Cord{owner: "runner-two"}

	assert.Equal(t, RunnerID("runner-one"), first.RunnerID())
	assert.Equal(t, RunnerID("runner-two"), second.RunnerID())
	assert.NotEqual(t, first.RunnerID(), second.RunnerID())
	assert.Empty(t, (*Cord)(nil).RunnerID())
}

func TestInspectRunConvertsSnapshot(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("snapshot-test", 60*60)
	submitted := time.Date(2026, time.January, 2, 3, 4, 5, 0, location)
	started := submitted.Add(time.Second)
	finished := started.Add(time.Second)
	runner := storage.RunnerID("runner-one")
	store := &snapshotStore{runReport: storage.RunReport{
		SubmittedAt: submitted, FirstStartedAt: &started, StateChangedAt: finished,
		FinishedAt: &finished, TerminalRunnerID: &runner, ID: "run-one", WorkflowName: "flow",
		State: storage.RunCompleted, Reason: storage.ReasonSucceeded,
		NodeCounts: storage.NodeStateCounts{
			Pending: 0, Ready: 0, Running: 0, RetryWait: 0, Completed: 1, Failed: 0, Canceled: 0,
		},
	}}
	runtime := openSnapshotRuntime(store)

	report, err := runtime.InspectRun(t.Context(), "run-one")

	require.NoError(t, err)
	assert.Equal(t, time.UTC, report.SubmittedAt.Location())
	assert.Equal(t, time.UTC, report.FirstStartedAt.Location())
	assert.Equal(t, RunnerID("runner-one"), *report.TerminalRunnerID)
	assert.Equal(t, RunStateCompleted, report.State)
	assert.Equal(t, ReasonSucceeded, report.Reason)
	assert.Equal(t, 1, report.NodeCounts.Completed)
}

func TestInspectRunValidationAndErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     context.Context
		is      error
		runtime *Cord
		name    string
		id      RunID
		want    string
	}{
		{
			name: "nil context", runtime: openSnapshotRuntime(&snapshotStore{}),
			ctx: nil, id: snapshotRunID, want: "context is nil",
		},
		{
			name: "empty run ID", runtime: openSnapshotRuntime(&snapshotStore{}),
			ctx: t.Context(), want: "run ID is empty",
		},
		{name: "nil runtime", ctx: t.Context(), id: snapshotRunID, want: "invalid runtime"},
		{
			name: "closed runtime", runtime: &Cord{store: &snapshotStore{}},
			ctx: t.Context(), id: snapshotRunID, want: "runtime closed",
		},
		{
			name: "not found", runtime: openSnapshotRuntime(&snapshotStore{runErr: storage.ErrRunNotFound}),
			ctx: t.Context(), id: snapshotRunID, is: ErrRunNotFound,
		},
		{
			name:    "incompatible storage",
			runtime: openSnapshotRuntime(&snapshotStore{runErr: storage.ErrRunIncompatible}),
			ctx:     t.Context(), id: snapshotRunID, is: ErrRunIncompatible,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.runtime.InspectRun(testCase.ctx, testCase.id)
			if testCase.want != "" {
				require.ErrorContains(t, err, testCase.want)
			}

			if testCase.is != nil {
				assert.ErrorIs(t, err, testCase.is)
			}
		})
	}
}

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

func TestNodePageTokenRejectsModificationAndVersion(t *testing.T) {
	t.Parallel()

	token, err := encodeNodePageToken(nodePageToken{
		RunID: snapshotRunID, State: NodeStateReady, LastNodeID: snapshotNodeID,
	})
	require.NoError(t, err)

	modified, err := decodeTokenWire(token)
	require.NoError(t, err)

	modified.Payload.LastNodeID = "modified"
	_, err = decodeNodePageToken(encodeTokenWire(t, &modified))
	require.ErrorContains(t, err, "checksum")

	unsupported, err := decodeTokenWire(token)
	require.NoError(t, err)

	unsupported.Version++
	_, err = decodeNodePageToken(encodeTokenWire(t, &unsupported))
	require.ErrorContains(t, err, "unsupported")
}

func TestNodePageTokenEncodingIsDeterministicAndBounded(t *testing.T) {
	t.Parallel()

	for _, token := range []nodePageToken{
		{RunID: snapshotRunID, LastNodeID: snapshotNodeID},
		{
			RunID: snapshotRunID, State: NodeStateFailed,
			Reason: ReasonFailureLeaseExpired, LastNodeID: "node-世界",
		},
	} {
		first, err := encodeNodePageToken(token)
		require.NoError(t, err)
		second, err := encodeNodePageToken(token)
		require.NoError(t, err)

		assert.Equal(t, first, second)
		assert.LessOrEqual(t, len(first), nodePageTokenMaxLen)
		decoded, err := decodeNodePageToken(first)
		require.NoError(t, err)
		assert.Equal(t, token, decoded)
	}

	_, err := decodeNodePageToken(string(make([]byte, nodePageTokenMaxLen+1)))
	assert.ErrorContains(t, err, "length")
}

func TestLifecycleModelProperties(t *testing.T) {
	t.Parallel()

	runStates := []RunState{
		RunStateRunning, RunStateCanceling, RunStateCompleted, RunStateFailed, RunStateCanceled,
	}
	nodeStates := []NodeState{
		NodeStatePending, NodeStateReady, NodeStateRunning, NodeStateRetryWait,
		NodeStateCompleted, NodeStateFailed, NodeStateCanceled,
	}
	reasons := []TerminalReason{
		"", ReasonSucceeded, ReasonCanceledByRequest, ReasonCanceledByRunFailure,
		ReasonFailureNonRetryable, ReasonFailureAttemptsExhausted,
		ReasonFailureLeaseExpired, ReasonLegacyUnknown, "future",
	}

	for _, state := range runStates {
		terminal, known := state.Terminal()
		assert.True(t, known)
		assert.Equal(t, state.IsKnown(), known)

		for _, reason := range reasons {
			if state.AllowsReason(reason) {
				assert.Equal(t, terminal, reason != "", "%s/%s", state, reason)
				assert.True(t, reason == "" || reason.IsKnown(), "%s/%s", state, reason)
			}
		}
	}

	for _, state := range nodeStates {
		terminal, known := state.Terminal()
		assert.True(t, known)
		assert.Equal(t, state.IsKnown(), known)

		for _, reason := range reasons {
			if state.AllowsReason(reason) {
				assert.Equal(t, terminal, reason != "", "%s/%s", state, reason)
				assert.True(t, reason == "" || reason.IsKnown(), "%s/%s", state, reason)
			}
		}
	}
}

func TestSnapshotStorageErrorsPreserveBackendErrors(t *testing.T) {
	t.Parallel()

	backendErr := errors.New("backend unavailable")
	store := &snapshotStore{runErr: backendErr, nodeErr: backendErr}
	runtime := openSnapshotRuntime(store)

	_, err := runtime.InspectRun(t.Context(), snapshotRunID)
	require.ErrorIs(t, err, backendErr)
	_, err = runtime.ListRunNodes(t.Context(), snapshotRunID, NodeQuery{})
	require.ErrorIs(t, err, backendErr)
}

func openSnapshotRuntime(store storage.Backend) *Cord {
	return &Cord{store: store, acceptingRuns: true}
}

func decodeTokenWire(token string) (nodePageTokenWire, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nodePageTokenWire{}, fmt.Errorf("decode token wire: %w", err)
	}

	var wire nodePageTokenWire

	if err := json.Unmarshal(decoded, &wire); err != nil {
		return nodePageTokenWire{}, fmt.Errorf("unmarshal token wire: %w", err)
	}

	return wire, nil
}

func encodeTokenWire(t *testing.T, wire *nodePageTokenWire) string {
	t.Helper()

	encoded, err := json.Marshal(wire)
	require.NoError(t, err)

	return base64.RawURLEncoding.EncodeToString(encoded)
}
