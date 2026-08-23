package storage_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func TestValidateRunPlanRejectsDuplicateEdges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		parentOrder int
	}{
		{name: "exact duplicate", parentOrder: 0},
		{name: "same persisted edge with different parent order", parentOrder: 1},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			plan := validationJoinPlan()
			plan.Edges = append(plan.Edges, storage.Edge{
				RunID:       plan.Run.ID,
				Parent:      plan.Edges[0].Parent,
				Child:       plan.Edges[0].Child,
				ParentOrder: testCase.parentOrder,
			})
			plan.Nodes[2].RemainingDeps++

			err := storage.ValidateRunPlan(&plan)
			if err == nil || err.Error() != `validate run plan: duplicate edge "left" -> "joined"` {
				t.Fatalf("ValidateRunPlan() error = %v", err)
			}
		})
	}
}

func TestValidateRunPlanAllowsDistinctOrderedParents(t *testing.T) {
	t.Parallel()

	plan := validationJoinPlan()
	if err := storage.ValidateRunPlan(&plan); err != nil {
		t.Fatalf("ValidateRunPlan() error = %v", err)
	}
}

func validationJoinPlan() storage.RunPlan {
	now := time.Now().UTC()

	const (
		runID  storage.RunID  = "run"
		joinID storage.NodeID = "joined"
	)

	return storage.RunPlan{
		Run: storage.Run{
			CreatedAt: now, UpdatedAt: now, CompletedAt: nil,
			ID: runID, WorkflowName: "join", DefinitionHash: "definition", TerminalNodeID: joinID,
			Status: storage.RunRunning, Input: nil, Output: nil, Error: nil,
			MaxAttempts: 1, RetryBaseDelay: time.Millisecond,
			RetryMaxDelay: time.Second, RetryPolicyVersion: 1,
		},
		Nodes: []storage.Node{
			validationNode(runID, "left", storage.NodeReady, now, 0),
			validationNode(runID, "right", storage.NodeReady, now, 0),
			validationNode(runID, joinID, storage.NodePending, now, 2),
		},
		Edges: []storage.Edge{
			{RunID: runID, Parent: "left", Child: joinID, ParentOrder: 0},
			{RunID: runID, Parent: "right", Child: joinID, ParentOrder: 1},
		},
	}
}

func validationNode(
	runID storage.RunID,
	nodeID storage.NodeID,
	status storage.NodeStatus,
	availableAt time.Time,
	remainingDependencies int,
) storage.Node {
	return storage.Node{
		AvailableAt: availableAt, CompletedAt: nil, StartedAt: nil,
		FunctionKey: "function", RunID: runID, ID: nodeID, SignatureHash: "signature", Status: status,
		Error: nil, Output: nil, Lease: storage.Lease{}, RemainingDeps: remainingDependencies, Attempt: 0,
	}
}
