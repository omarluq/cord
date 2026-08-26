package conformance

import (
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func singleNodePlan(runID storage.RunID, name string) storage.RunPlan {
	const maxAttempts = 3

	now := time.Now().UTC().Add(-time.Second)

	return storage.RunPlan{
		Edges: nil,
		Run: storage.Run{
			CreatedAt: now, UpdatedAt: now, CompletedAt: nil, StartedAt: nil,
			TerminalReason: nil, TerminalRunnerID: nil,
			ID: runID, WorkflowName: name, DefinitionHash: "definition",
			IdempotencyKey: nil, SubmissionFingerprint: nil, TerminalNodeID: conformanceNodeID,
			Status: storage.RunRunning, Input: []byte(`"input"`), Output: nil, Error: nil,
			MaxAttempts: maxAttempts, RetryBaseDelay: time.Millisecond,
			RetryMaxDelay: time.Second, RetryPolicyVersion: 1,
		},
		Nodes: []storage.Node{
			conformanceNode(runID, conformanceNodeID, stepFunctionKey, "signature", storage.NodeReady, now, 0),
		},
	}
}

func joinPlan(runID storage.RunID) storage.RunPlan {
	plan := singleNodePlan(runID, "join")
	plan.Run.TerminalNodeID = joinNodeID
	plan.Nodes = []storage.Node{
		conformanceNode(runID, leftNodeID, leftFunctionKey, "left", storage.NodeReady, plan.Run.CreatedAt, 0),
		conformanceNode(runID, rightNodeID, rightFunctionKey, "right", storage.NodeReady, plan.Run.CreatedAt, 0),
		conformanceNode(
			runID, joinNodeID, joinFunctionKey, "join",
			storage.NodePending, plan.Run.CreatedAt, joinDependencies,
		),
	}
	plan.Edges = []storage.Edge{
		{RunID: runID, Parent: rightNodeID, Child: joinNodeID, ParentOrder: 0},
		{RunID: runID, Parent: leftNodeID, Child: joinNodeID, ParentOrder: 1},
	}

	return plan
}

func conformanceNode(
	runID storage.RunID,
	identifier storage.NodeID,
	functionKey, signature string,
	status storage.NodeStatus,
	availableAt time.Time,
	remainingDependencies int,
) storage.Node {
	return storage.Node{
		AvailableAt: availableAt, CompletedAt: nil, StartedAt: nil,
		StateChangedAt: nil, LastStartedAt: nil,
		LastRunnerID: nil, TerminalReason: nil,
		FunctionKey: functionKey, RunID: runID, ID: identifier, SignatureHash: signature,
		Status: status, Error: nil, Output: nil, Lease: storage.Lease{},
		RemainingDeps: remainingDependencies, Attempt: 0,
	}
}

func nodeQuery(state *storage.NodeStatus, reason *storage.TerminalReason, pageSize int) storage.NodeQuery {
	return storage.NodeQuery{
		State: state, Reason: reason, ContinuationToken: "", PageSize: pageSize,
	}
}

func paginatedPlan(runID storage.RunID) storage.RunPlan {
	const nodeCount = 5

	plan := singleNodePlan(runID, "node-pagination")
	plan.Run.TerminalNodeID = "node-04"
	plan.Nodes = make([]storage.Node, 0, nodeCount)
	plan.Edges = make([]storage.Edge, 0, nodeCount-1)

	for index := range nodeCount {
		nodeID := storage.NodeID(fmt.Sprintf("node-%02d", index))
		status := storage.NodePending
		remainingDependencies := 1

		if index == 0 {
			status = storage.NodeReady
			remainingDependencies = 0
		}

		plan.Nodes = append(plan.Nodes, conformanceNode(
			runID, nodeID, stepFunctionKey, "signature", status, plan.Run.CreatedAt, remainingDependencies,
		))
		if index > 0 {
			plan.Edges = append(plan.Edges, storage.Edge{
				RunID: runID, Parent: storage.NodeID(fmt.Sprintf("node-%02d", index-1)),
				Child: nodeID, ParentOrder: 0,
			})
		}
	}

	return plan
}

func nodeIDs(nodes []storage.NodeReport) []storage.NodeID {
	identifiers := make([]storage.NodeID, len(nodes))
	for index := range nodes {
		identifiers[index] = nodes[index].NodeID
	}

	return identifiers
}
