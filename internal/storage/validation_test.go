package storage_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

type runPlanValidationTest struct {
	mutate  func(*storage.RunPlan)
	name    string
	wantErr string
}

func validationRunPlanTests() []runPlanValidationTest {
	tests := runValidationTests()
	tests = append(tests, nodeValidationTests()...)
	tests = append(tests, nodeStateValidationTests()...)
	tests = append(tests, edgeValidationTests()...)

	return tests
}

func runValidationTests() []runPlanValidationTest {
	return []runPlanValidationTest{
		{name: "valid", mutate: func(*storage.RunPlan) {}, wantErr: ""},
		{
			name:    "empty run ID",
			mutate:  func(plan *storage.RunPlan) { plan.Run.ID = "" },
			wantErr: "validate run plan: run ID is empty",
		},
		{
			name:    "empty workflow name",
			mutate:  func(plan *storage.RunPlan) { plan.Run.WorkflowName = "" },
			wantErr: "validate run plan: workflow name is empty",
		},
		{
			name:    "empty definition hash",
			mutate:  func(plan *storage.RunPlan) { plan.Run.DefinitionHash = "" },
			wantErr: "validate run plan: definition hash is empty",
		},
		{
			name:    "empty terminal node ID",
			mutate:  func(plan *storage.RunPlan) { plan.Run.TerminalNodeID = "" },
			wantErr: "validate run plan: terminal node ID is empty",
		},
		{
			name:    "invalid run status",
			mutate:  func(plan *storage.RunPlan) { plan.Run.Status = storage.RunCompleted },
			wantErr: `validate run plan: run must initially be "running"`,
		},
		{
			name:    "nil input",
			mutate:  func(plan *storage.RunPlan) { plan.Run.Input = nil },
			wantErr: "validate run plan: run input is nil",
		},
		{
			name:    "initial run output",
			mutate:  func(plan *storage.RunPlan) { plan.Run.Output = []byte("output") },
			wantErr: "validate run plan: run output must initially be unset",
		},
		{
			name:    "initial run error",
			mutate:  func(plan *storage.RunPlan) { plan.Run.Error = []byte("error") },
			wantErr: "validate run plan: run error must initially be unset",
		},
		{
			name:    "initial run completion",
			mutate:  func(plan *storage.RunPlan) { plan.Run.CompletedAt = new(time.Time) },
			wantErr: "validate run plan: run completion time must initially be unset",
		},
		{
			name:    "zero creation time",
			mutate:  func(plan *storage.RunPlan) { plan.Run.CreatedAt = time.Time{} },
			wantErr: "validate run plan: run creation time is zero",
		},
		{
			name:    "zero update time",
			mutate:  func(plan *storage.RunPlan) { plan.Run.UpdatedAt = time.Time{} },
			wantErr: "validate run plan: run update time is zero",
		},
		{
			name:    "future retry policy version",
			mutate:  func(plan *storage.RunPlan) { plan.Run.RetryPolicyVersion = 2 },
			wantErr: "validate run plan: unsupported retry policy version 2 (want 1)",
		},
	}
}

func nodeValidationTests() []runPlanValidationTest {
	return []runPlanValidationTest{
		{
			name:    "empty node ID",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].ID = "" },
			wantErr: "validate run plan: node ID is empty",
		},
		{
			name:    "duplicate node",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[1].ID = plan.Nodes[0].ID },
			wantErr: `validate run plan: duplicate node "left"`,
		},
		{
			name:    "empty function key",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].FunctionKey = "" },
			wantErr: `validate run plan: node "left" function key is empty`,
		},
		{
			name:    "empty signature hash",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].SignatureHash = "" },
			wantErr: `validate run plan: node "left" signature hash is empty`,
		},
		{
			name:    "zero availability time",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].AvailableAt = time.Time{} },
			wantErr: `validate run plan: node "left" availability time is zero`,
		},
		{
			name:    "initial node output",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Output = []byte("output") },
			wantErr: `validate run plan: node "left" output must initially be unset`,
		},
		{
			name:    "initial node error",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Error = []byte("error") },
			wantErr: `validate run plan: node "left" error must initially be unset`,
		},
		{
			name:    "initial node start",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].StartedAt = new(time.Time) },
			wantErr: `validate run plan: node "left" start time must initially be unset`,
		},
		{
			name:    "initial node completion",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].CompletedAt = new(time.Time) },
			wantErr: `validate run plan: node "left" completion time must initially be unset`,
		},
	}
}

func nodeStateValidationTests() []runPlanValidationTest {
	return []runPlanValidationTest{
		{
			name:    "initial lease owner",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Lease.Owner = "worker" },
			wantErr: `validate run plan: node "left" lease owner must initially be empty`,
		},
		{
			name:    "initial lease generation",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Lease.Generation = 1 },
			wantErr: `validate run plan: node "left" lease generation must initially be zero`,
		},
		{
			name: "initial lease expiry",
			mutate: func(plan *storage.RunPlan) {
				plan.Nodes[0].Lease.ExpiresAt = plan.Nodes[0].AvailableAt.Add(time.Second)
			},
			wantErr: `validate run plan: node "left" lease expiry must initially be unset`,
		},
		{
			name:    "initial lease remaining",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Lease.Remaining = time.Second },
			wantErr: `validate run plan: node "left" lease remaining time must initially be zero`,
		},
		{
			name:    "initial attempt",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Attempt = 1 },
			wantErr: `validate run plan: node "left" attempt must initially be zero`,
		},
		{
			name:    "negative dependency count",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].RemainingDeps = -1 },
			wantErr: `validate run plan: node "left" dependency count must be non-negative`,
		},
		{
			name:    "dependency count mismatch",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[2].RemainingDeps = 1 },
			wantErr: `validate run plan: node "joined" dependency count does not match edges`,
		},
		{
			name:    "ready node with dependencies",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[2].Status = storage.NodeReady },
			wantErr: `validate run plan: node "joined" must initially be "pending"`,
		},
		{
			name:    "pending root node",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Status = storage.NodePending },
			wantErr: `validate run plan: node "left" must initially be "ready"`,
		},
	}
}

func edgeValidationTests() []runPlanValidationTest {
	return []runPlanValidationTest{
		{name: "duplicate edge", mutate: func(plan *storage.RunPlan) {
			plan.Edges = append(plan.Edges, plan.Edges[0])
			plan.Nodes[2].RemainingDeps++
		}, wantErr: `validate run plan: duplicate edge "left" -> "joined"`},
		{name: "duplicate edge with different order", mutate: func(plan *storage.RunPlan) {
			edge := plan.Edges[0]
			edge.ParentOrder = 2
			plan.Edges = append(plan.Edges, edge)
			plan.Nodes[2].RemainingDeps++
		}, wantErr: `validate run plan: duplicate edge "left" -> "joined"`},
		{
			name:    "negative parent order",
			mutate:  func(plan *storage.RunPlan) { plan.Edges[0].ParentOrder = -1 },
			wantErr: `validate run plan: edge "left" -> "joined" parent order must be non-negative`,
		},
		{
			name:    "duplicate parent order",
			mutate:  func(plan *storage.RunPlan) { plan.Edges[1].ParentOrder = 0 },
			wantErr: `validate run plan: node "joined" has duplicate parent order 0`,
		},
		{
			name:    "noncontiguous parent order",
			mutate:  func(plan *storage.RunPlan) { plan.Edges[1].ParentOrder = 2 },
			wantErr: `validate run plan: node "joined" parent order values must be contiguous from zero (missing 1)`,
		},
	}
}

func TestValidateRunPlan(t *testing.T) {
	t.Parallel()

	tests := validationRunPlanTests()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			plan := validationJoinPlan()
			testCase.mutate(&plan)

			err := storage.ValidateRunPlan(&plan)
			if testCase.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateRunPlan() error = %v", err)
				}

				return
			}

			if err == nil || err.Error() != testCase.wantErr {
				t.Fatalf("ValidateRunPlan() error = %v, want %q", err, testCase.wantErr)
			}
		})
	}
}

func TestValidateRunPlanRejectsNilPlan(t *testing.T) {
	t.Parallel()

	err := storage.ValidateRunPlan(nil)
	if err == nil || err.Error() != "validate run plan: plan is nil" {
		t.Fatalf("ValidateRunPlan(nil) error = %v", err)
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
			Status: storage.RunRunning, Input: []byte("null"), Output: nil, Error: nil,
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
