package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStore_CreateRunRejectsInvalidPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*storage.RunPlan)
		name   string
	}{
		{name: "nil plan", mutate: nil},
		{
			name: "invalid retry policy",
			mutate: func(plan *storage.RunPlan) {
				plan.Run.MaxAttempts = 0
			},
		},
		{
			name: "mismatched node run",
			mutate: func(plan *storage.RunPlan) {
				plan.Nodes[0].RunID = "another-run"
			},
		},
		{
			name: "mismatched edge run",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges[0].RunID = "another-run"
			},
		},
		{
			name: "missing terminal node",
			mutate: func(plan *storage.RunPlan) {
				plan.Run.TerminalNodeID = "missing"
			},
		},
		{
			name: "missing edge endpoint",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges[0].Parent = "missing"
			},
		},
		{
			name: "missing edge child",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges[0].Child = "absent"
			},
		},
		{
			name: "incorrect dependency count",
			mutate: func(plan *storage.RunPlan) {
				plan.Nodes[1].RemainingDeps = 0
			},
		},
		{
			name: "incorrect initial status",
			mutate: func(plan *storage.RunPlan) {
				plan.Nodes[0].Status = storage.NodePending
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)

			var plan *storage.RunPlan

			if testCase.mutate != nil {
				candidate := validPlan(time.Now().UTC(), "invalid-run")
				testCase.mutate(&candidate)
				plan = &candidate
			}

			requireCreateRunError(t.Context(), t, store, plan)
			assert.Equal(t, 0, rowCount(t, database, runsTable))
		})
	}
}

func TestStore_CreateRunRejectsEmptyRunIDAndDuplicateNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*storage.RunPlan)
		name   string
	}{
		{name: "empty run ID", mutate: func(plan *storage.RunPlan) { plan.Run.ID = "" }},
		{name: "duplicate node", mutate: func(plan *storage.RunPlan) {
			plan.Nodes[1].ID = plan.Nodes[0].ID
			plan.Run.TerminalNodeID = plan.Nodes[0].ID
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), "structurally-invalid")
			testCase.mutate(&plan)
			requireCreateRunError(t.Context(), t, store, &plan)
		})
	}
}

func TestStore_CreateRunRejectsCycles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*storage.RunPlan)
		name   string
	}{
		{
			name: "two nodes",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges = append(plan.Edges, storage.Edge{
					RunID:       plan.Run.ID,
					Parent:      plan.Nodes[1].ID,
					Child:       plan.Nodes[0].ID,
					ParentOrder: 0,
				})
				plan.Nodes[0].RemainingDeps = 1
			},
		},
		{
			name: "self cycle",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges = append(plan.Edges, storage.Edge{
					RunID:       plan.Run.ID,
					Parent:      plan.Nodes[0].ID,
					Child:       plan.Nodes[0].ID,
					ParentOrder: 0,
				})
				plan.Nodes[0].RemainingDeps = 1
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), "cycle-run")
			testCase.mutate(&plan)

			requireCreateRunErrorContains(t.Context(), t, store, &plan, "cycle")
			assert.Equal(t, 0, rowCount(t, database, runsTable))
		})
	}
}
