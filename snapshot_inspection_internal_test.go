package cord

import (
	"context"
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
