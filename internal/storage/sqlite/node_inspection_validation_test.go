package sqlite_test

import (
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStore_ListRunNodesAllowsUnclaimedCurrentNode(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "unclaimed-current-node")
	requireCreateRun(t.Context(), t, store, &plan)
	_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET state_changed_at = available_at
		WHERE run_id = ?`, plan.Run.ID)
	require.NoError(t, err)

	page, err := store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
	require.NoError(t, err)
	require.Len(t, page.Nodes, len(plan.Nodes))

	for _, node := range page.Nodes {
		assert.Zero(t, node.Attempt)
		assert.Nil(t, node.FirstStartedAt)
		assert.Nil(t, node.LastStartedAt)
		assert.Nil(t, node.RunnerID)
	}
}

func TestStore_ListRunNodesRejectsUnclaimedCurrentNodeStartMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                          string
		firstStart, lastStart, runner bool
	}{
		{name: "first start", firstStart: true, lastStart: false, runner: false},
		{name: "last start", firstStart: false, lastStart: true, runner: false},
		{name: "runner", firstStart: false, lastStart: false, runner: true},
		{name: "complete start metadata", firstStart: true, lastStart: true, runner: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), storage.RunID("unclaimed-metadata-"+testCase.name))
			requireCreateRun(t.Context(), t, store, &plan)

			_, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
				SET state_changed_at = available_at,
					started_at = CASE WHEN ? THEN available_at ELSE NULL END,
					last_started_at = CASE WHEN ? THEN available_at ELSE NULL END,
					last_runner_id = CASE WHEN ? THEN 'worker' ELSE NULL END
				WHERE run_id = ? AND node_id = ?`,
				testCase.firstStart, testCase.lastStart, testCase.runner,
				plan.Run.ID, compileNode)
			require.NoError(t, err)

			_, err = store.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{})
			assert.ErrorIs(t, err, storage.ErrRunIncompatible)
		})
	}
}

func TestStore_ListRunNodesRejectsInvalidFilters(t *testing.T) {
	t.Parallel()

	_, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "invalid-filters")
	requireCreateRun(t.Context(), t, store, &plan)

	unknown := storage.NodeStatus("unknown")
	state := storage.NodeReady
	reason := storage.ReasonSucceeded
	tests := []struct {
		name      string
		wantError string
		query     storage.NodeQuery
	}{
		{
			name: "unknown state",
			query: storage.NodeQuery{
				State: &unknown, Reason: nil, ContinuationToken: "", PageSize: 0,
			},
			wantError: `list run nodes: unknown state "unknown"`,
		},
		{
			name: "state reason mismatch",
			query: storage.NodeQuery{
				State: &state, Reason: &reason, ContinuationToken: "", PageSize: 0,
			},
			wantError: `list run nodes: reason "succeeded" is invalid for state "ready"`,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := store.ListRunNodes(t.Context(), plan.Run.ID, testCase.query)
			require.EqualError(t, err, testCase.wantError)
		})
	}
}

func inspectionLinearPlan(runID storage.RunID, count int) storage.RunPlan {
	now := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	plan := validPlan(now, runID)
	plan.Nodes = make([]storage.Node, count)

	plan.Edges = make([]storage.Edge, 0, count-1)
	for index := range count {
		nodeID := storage.NodeID(fmt.Sprintf("node-%02d", index))
		status := storage.NodePending
		remaining := 1

		if index == 0 {
			status = storage.NodeReady
			remaining = 0
		}

		plan.Nodes[index] = newNode(runID, nodeID, "inspect.Step", "signature", status, now, remaining)
		if index > 0 {
			plan.Edges = append(plan.Edges, storage.Edge{
				RunID: runID, Parent: plan.Nodes[index-1].ID, Child: nodeID, ParentOrder: 0,
			})
		}
	}

	plan.Run.TerminalNodeID = plan.Nodes[count-1].ID

	return plan
}
