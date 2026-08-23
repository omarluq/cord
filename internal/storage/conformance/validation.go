package conformance

import (
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// RunPlanValidationTest describes one run-plan validation contract case.
type RunPlanValidationTest struct {
	Mutate  func(*storage.RunPlan)
	Name    string
	WantErr string
}

// ValidationRunPlanTests returns the backend-neutral run-plan validation cases.
func ValidationRunPlanTests() []RunPlanValidationTest {
	tests := runValidationTests()
	tests = append(tests, nodeValidationTests()...)
	tests = append(tests, nodeStateValidationTests()...)
	tests = append(tests, edgeValidationTests()...)

	return tests
}

func runValidationTests() []RunPlanValidationTest {
	return []RunPlanValidationTest{
		{
			Name: "valid",
			Mutate: func(*storage.RunPlan) {
				// The valid case intentionally leaves the plan unchanged.
			},
			WantErr: "",
		},
		{
			Name:    "empty run ID",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.ID = "" },
			WantErr: "validate run plan: run ID is empty",
		},
		{
			Name:    "empty workflow name",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.WorkflowName = "" },
			WantErr: "validate run plan: workflow name is empty",
		},
		{
			Name:    "empty definition hash",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.DefinitionHash = "" },
			WantErr: "validate run plan: definition hash is empty",
		},
		{
			Name:    "empty terminal node ID",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.TerminalNodeID = "" },
			WantErr: "validate run plan: terminal node ID is empty",
		},
		{
			Name:    "invalid run status",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.Status = storage.RunCompleted },
			WantErr: `validate run plan: run must initially be "running"`,
		},
		{
			Name:    "nil input",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.Input = nil },
			WantErr: "validate run plan: run input is nil",
		},
		{
			Name:    "initial run output",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.Output = []byte("output") },
			WantErr: "validate run plan: run output must initially be unset",
		},
		{
			Name:    "initial run error",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.Error = []byte("error") },
			WantErr: "validate run plan: run error must initially be unset",
		},
		{
			Name:    "initial run completion",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.CompletedAt = new(time.Time) },
			WantErr: "validate run plan: run completion time must initially be unset",
		},
		{
			Name:    "zero creation time",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.CreatedAt = time.Time{} },
			WantErr: "validate run plan: run creation time is zero",
		},
		{
			Name:    "zero update time",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.UpdatedAt = time.Time{} },
			WantErr: "validate run plan: run update time is zero",
		},
		{
			Name:    "future retry policy version",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.RetryPolicyVersion = 2 },
			WantErr: "validate run plan: unsupported retry policy version 2 (want 1)",
		},
	}
}

func nodeValidationTests() []RunPlanValidationTest {
	return []RunPlanValidationTest{
		{
			Name:    "empty node ID",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].ID = "" },
			WantErr: "validate run plan: node ID is empty",
		},
		{
			Name:    "duplicate node",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[1].ID = plan.Nodes[0].ID },
			WantErr: `validate run plan: duplicate node "left"`,
		},
		{
			Name:    "empty function key",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].FunctionKey = "" },
			WantErr: `validate run plan: node "left" function key is empty`,
		},
		{
			Name:    "empty signature hash",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].SignatureHash = "" },
			WantErr: `validate run plan: node "left" signature hash is empty`,
		},
		{
			Name:    "zero availability time",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].AvailableAt = time.Time{} },
			WantErr: `validate run plan: node "left" availability time is zero`,
		},
		{
			Name:    "initial node output",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Output = []byte("output") },
			WantErr: `validate run plan: node "left" output must initially be unset`,
		},
		{
			Name:    "initial node error",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Error = []byte("error") },
			WantErr: `validate run plan: node "left" error must initially be unset`,
		},
		{
			Name:    "initial node start",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].StartedAt = new(time.Time) },
			WantErr: `validate run plan: node "left" start time must initially be unset`,
		},
		{
			Name:    "initial node completion",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].CompletedAt = new(time.Time) },
			WantErr: `validate run plan: node "left" completion time must initially be unset`,
		},
	}
}

func nodeStateValidationTests() []RunPlanValidationTest {
	return []RunPlanValidationTest{
		{
			Name:    "initial lease owner",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Lease.Owner = "worker" },
			WantErr: `validate run plan: node "left" lease owner must initially be empty`,
		},
		{
			Name:    "initial lease generation",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Lease.Generation = 1 },
			WantErr: `validate run plan: node "left" lease generation must initially be zero`,
		},
		{
			Name: "initial lease expiry",
			Mutate: func(plan *storage.RunPlan) {
				plan.Nodes[0].Lease.ExpiresAt = plan.Nodes[0].AvailableAt.Add(time.Second)
			},
			WantErr: `validate run plan: node "left" lease expiry must initially be unset`,
		},
		{
			Name:    "initial lease remaining",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Lease.Remaining = time.Second },
			WantErr: `validate run plan: node "left" lease remaining time must initially be zero`,
		},
		{
			Name:    "initial attempt",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Attempt = 1 },
			WantErr: `validate run plan: node "left" attempt must initially be zero`,
		},
		{
			Name:    "negative dependency count",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].RemainingDeps = -1 },
			WantErr: `validate run plan: node "left" dependency count must be non-negative`,
		},
		{
			Name:    "dependency count mismatch",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[2].RemainingDeps = 1 },
			WantErr: `validate run plan: node "joined" dependency count does not match edges`,
		},
		{
			Name:    "ready node with dependencies",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[2].Status = storage.NodeReady },
			WantErr: `validate run plan: node "joined" must initially be "pending"`,
		},
		{
			Name:    "pending root node",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].Status = storage.NodePending },
			WantErr: `validate run plan: node "left" must initially be "ready"`,
		},
	}
}

func edgeValidationTests() []RunPlanValidationTest {
	return []RunPlanValidationTest{
		{Name: "duplicate edge", Mutate: func(plan *storage.RunPlan) {
			plan.Edges = append(plan.Edges, plan.Edges[0])
			plan.Nodes[2].RemainingDeps++
		}, WantErr: `validate run plan: duplicate edge "left" -> "joined"`},
		{Name: "duplicate edge with different order", Mutate: func(plan *storage.RunPlan) {
			edge := plan.Edges[0]
			edge.ParentOrder = 2
			plan.Edges = append(plan.Edges, edge)
			plan.Nodes[2].RemainingDeps++
		}, WantErr: `validate run plan: duplicate edge "left" -> "joined"`},
		{
			Name:    "negative parent order",
			Mutate:  func(plan *storage.RunPlan) { plan.Edges[0].ParentOrder = -1 },
			WantErr: `validate run plan: edge "left" -> "joined" parent order must be non-negative`,
		},
		{
			Name:    "duplicate parent order",
			Mutate:  func(plan *storage.RunPlan) { plan.Edges[1].ParentOrder = 0 },
			WantErr: `validate run plan: node "joined" has duplicate parent order 0`,
		},
		{
			Name:    "noncontiguous parent order",
			Mutate:  func(plan *storage.RunPlan) { plan.Edges[1].ParentOrder = 2 },
			WantErr: `validate run plan: node "joined" parent order values must be contiguous from zero (missing 1)`,
		},
	}
}

// ValidationJoinPlan returns a valid fan-in plan for validation tests.
func ValidationJoinPlan(runID storage.RunID) storage.RunPlan {
	now := time.Now().UTC()

	const (
		joinID           storage.NodeID = "joined"
		joinDependencies int            = 2
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
			validationNode(runID, joinID, storage.NodePending, now, joinDependencies),
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
