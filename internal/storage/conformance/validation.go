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
	tests := runIdentityValidationTests()
	tests = append(tests, idempotencyValidationTests()...)
	tests = append(tests, lifecycleMetadataValidationTests()...)
	tests = append(tests, runInitialStateValidationTests()...)
	tests = append(tests, nodeValidationTests()...)
	tests = append(tests, nodeStateValidationTests()...)
	tests = append(tests, edgeValidationTests()...)

	return tests
}

// ValidationJoinPlan returns a valid fan-in plan for validation tests.
func ValidationJoinPlan(runID storage.RunID) storage.RunPlan {
	now := time.Now().UTC()

	const (
		joinID           storage.NodeID = "joined"
		joinDependencies int            = 2
	)

	plan := storage.RunPlan{
		Run: storage.Run{
			CreatedAt: now, UpdatedAt: now, CompletedAt: nil, StartedAt: nil,
			TerminalReason: nil, TerminalRunnerID: nil,
			ID: runID, WorkflowName: "join", DefinitionHash: "definition",
			IdempotencyKey: nil, SubmissionFingerprint: nil, TerminalNodeID: joinID,
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

	return plan
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
		StateChangedAt: nil, LastStartedAt: nil,
		LastRunnerID: nil, TerminalReason: nil,
		FunctionKey: "function", RunID: runID, ID: nodeID, SignatureHash: "signature", Status: status,
		Error: nil, Output: nil, Lease: storage.Lease{}, RemainingDeps: remainingDependencies, Attempt: 0,
	}
}
