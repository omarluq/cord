package conformance

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func cancellationPlan(runID storage.RunID) storage.RunPlan {
	plan := singleNodePlan(runID, "cancellation-states")
	now := plan.Run.CreatedAt

	const (
		pendingDependencies = 4
		retryingParentOrder = 2
		readyParentOrder    = 3
		retryingOffset      = 2 * time.Millisecond
		readyOffset         = 3 * time.Millisecond
	)

	plan.Run.TerminalNodeID = terminalNodeName
	plan.Nodes = []storage.Node{
		conformanceNode(
			runID, completedNodeName, completedNodeName, "completed-signature", storage.NodeReady, now, 0,
		),
		conformanceNode(
			runID, runningNodeName, runningNodeName, "running-signature",
			storage.NodeReady, now.Add(time.Millisecond), 0,
		),
		conformanceNode(
			runID, retryingNodeName, retryingNodeName, "retry-signature",
			storage.NodeReady, now.Add(retryingOffset), 0,
		),
		conformanceNode(
			runID, readyNodeName, readyNodeName, "ready-signature", storage.NodeReady, now.Add(readyOffset), 0,
		),
		conformanceNode(
			runID, pendingNodeName, pendingNodeName, "pending-signature",
			storage.NodePending, now, pendingDependencies,
		),
		conformanceNode(
			runID, terminalNodeName, terminalNodeName, "terminal-signature", storage.NodePending, now, 1,
		),
	}
	plan.Edges = []storage.Edge{
		{RunID: runID, Parent: completedNodeName, Child: pendingNodeName, ParentOrder: 0},
		{RunID: runID, Parent: runningNodeName, Child: pendingNodeName, ParentOrder: 1},
		{RunID: runID, Parent: retryingNodeName, Child: pendingNodeName, ParentOrder: retryingParentOrder},
		{RunID: runID, Parent: readyNodeName, Child: pendingNodeName, ParentOrder: readyParentOrder},
		{RunID: runID, Parent: pendingNodeName, Child: terminalNodeName, ParentOrder: 0},
	}

	return plan
}

func requireCancellationFences(
	t *testing.T,
	backend storage.Backend,
	running *storage.Claim,
	retrying *storage.Claim,
) {
	t.Helper()

	accepted, err := backend.CompleteNode(
		t.Context(), running.RunID, running.NodeID, running.Lease, []byte(`"late"`),
	)
	requireRejected(t, "completion after cancellation", accepted, err)
	accepted, err = backend.RetryNode(
		t.Context(), running.RunID, running.NodeID, running.Lease, []byte(`"late"`), 0,
	)
	requireRejected(t, retryAfterCancellation, accepted, err)
	accepted, err = backend.FailNode(
		t.Context(), running.RunID, running.NodeID, running.Lease, []byte(`"late"`),
		storage.ReasonFailureNonRetryable,
	)
	requireRejected(t, "failure after cancellation", accepted, err)

	accepted, remaining, err := backend.HeartbeatNode(
		t.Context(), running.RunID, running.NodeID, running.Lease, time.Minute,
	)
	if err != nil || accepted || remaining != 0 {
		t.Fatalf("heartbeat after cancellation: accepted=%v remaining=%s err=%v", accepted, remaining, err)
	}

	accepted, err = backend.CompleteNode(
		t.Context(), retrying.RunID, retrying.NodeID, retrying.Lease, []byte(`"late retry"`),
	)
	requireRejected(t, "retry-wait completion after cancellation", accepted, err)
}

func requireCancellationOutcome(
	t *testing.T,
	got storage.CancellationOutcome,
	err error,
	want storage.CancellationOutcome,
) {
	t.Helper()

	if err != nil || got != want {
		t.Fatalf("CancelRun() = (%q, %v), want (%q, nil)", got, err, want)
	}
}

func mustClaimFunction(
	t *testing.T,
	backend storage.Backend,
	owner, functionKey, signature string,
) *storage.Claim {
	t.Helper()

	claim, claimed, err := backend.ClaimReadyNodeForFunctions(
		t.Context(), owner, time.Minute,
		[]storage.FunctionRegistration{{Key: functionKey, Signature: signature}},
	)
	if err != nil || !claimed || claim == nil {
		t.Fatalf("claim function %q: claim=%#v claimed=%v err=%v", functionKey, claim, claimed, err)
	}

	return claim
}

func requireClaimNode(t *testing.T, claim *storage.Claim, want storage.NodeID) {
	t.Helper()

	if claim.NodeID != want {
		t.Fatalf("claimed node = %q, want %q", claim.NodeID, want)
	}
}
