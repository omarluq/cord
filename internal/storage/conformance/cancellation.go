package conformance

import (
	"reflect"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const retryAfterCancellation = "retry after cancellation"

func runCancellationOutcomes(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "cancellation-outcomes")

	outcome, err := opened.backend.CancelRun(t.Context(), "missing-run")
	requireCancellationOutcome(t, outcome, err, storage.CancellationNotFound)

	canceled := singleNodePlan("cancel-outcome-canceled", "cancel-outcome-canceled")
	if err = opened.backend.CreateRun(t.Context(), &canceled); err != nil {
		t.Fatal(err)
	}

	outcome, err = opened.backend.CancelRun(t.Context(), canceled.Run.ID)
	requireCancellationOutcome(t, outcome, err, storage.CancellationCanceled)
	outcome, err = opened.backend.CancelRun(t.Context(), canceled.Run.ID)
	requireCancellationOutcome(t, outcome, err, storage.CancellationAlreadyCanceled)

	for _, terminal := range []struct {
		transition func(storage.Backend, *storage.Claim) (bool, error)
		name       string
	}{
		{name: "completed", transition: func(backend storage.Backend, claim *storage.Claim) (bool, error) {
			return backend.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"done"`))
		}},
		{name: "failed", transition: func(backend storage.Backend, claim *storage.Claim) (bool, error) {
			return backend.FailNode(
				t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"failed"`),
				storage.ReasonFailureNonRetryable,
			)
		}},
	} {
		t.Run(terminal.name, func(t *testing.T) {
			plan := singleNodePlan(storage.RunID("cancel-outcome-"+terminal.name), "cancel-outcome-"+terminal.name)
			if createErr := opened.backend.CreateRun(t.Context(), &plan); createErr != nil {
				t.Fatal(createErr)
			}

			claim := mustClaim(t, opened.backend, "terminal-worker")
			accepted, transitionErr := terminal.transition(opened.backend, claim)
			requireAccepted(t, terminal.name, accepted, transitionErr)

			terminalOutcome, cancelErr := opened.backend.CancelRun(t.Context(), plan.Run.ID)
			requireCancellationOutcome(t, terminalOutcome, cancelErr, storage.CancellationFinished)
		})
	}
}

func runCancellationStatesAndFences(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "cancellation-states-fences")

	plan := cancellationPlan("cancel-states-fences")
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	completed := mustClaimFunction(
		t, opened.backend, "completed-worker", completedNodeName, "completed-signature",
	)
	requireClaimNode(t, completed, "completed")
	accepted, err := opened.backend.CompleteNode(
		t.Context(), completed.RunID, completed.NodeID, completed.Lease, []byte(`"done"`),
	)
	requireAccepted(t, "complete preserved node", accepted, err)

	running := mustClaimFunction(
		t, opened.backend, "running-worker", runningNodeName, "running-signature",
	)
	requireClaimNode(t, running, runningNodeName)
	retrying := mustClaimFunction(
		t, opened.backend, "retry-worker", retryingNodeName, "retry-signature",
	)
	requireClaimNode(t, retrying, "retrying")
	accepted, err = opened.backend.RetryNode(
		t.Context(), retrying.RunID, retrying.NodeID, retrying.Lease, []byte(`"retry"`), time.Hour,
	)
	requireAccepted(t, "put node in retry wait", accepted, err)

	outcome, err := opened.backend.CancelRun(t.Context(), plan.Run.ID)
	requireCancellationOutcome(t, outcome, err, storage.CancellationCanceled)

	result, err := opened.backend.GetRunResult(t.Context(), plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	requireRunIdentity(t, &result, &plan)

	if result.Status != storage.RunCanceled {
		t.Fatalf("canceled run status = %q, want %q", result.Status, storage.RunCanceled)
	}

	states, err := harness.LoadNodeStates(t.Context(), opened.database, plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	canceledNodes := []storage.NodeID{
		runningNodeName, readyNodeName, pendingNodeName, retryingNodeName, terminalNodeName,
	}
	for _, nodeID := range canceledNodes {
		state := states[nodeID]
		if state.Status != storage.NodeCanceled || state.LeaseOwner != "" || state.HasLeaseExpiry {
			t.Fatalf("canceled node %q state = %#v", nodeID, state)
		}
	}

	if state := states["completed"]; state.Status != storage.NodeCompleted {
		t.Fatalf("completed node state = %#v, want completed", state)
	}

	requireCancellationFences(t, opened.backend, running, retrying)
	claim, claimed, claimErr := claimAny(t.Context(), opened.backend, "after-cancellation")
	requireNotClaimed(t, claim, claimed, claimErr)
}

func runDeterministicCancellationOrderings(t *testing.T, harness Harness) {
	t.Helper()

	tests := []struct {
		run  func(*testing.T, Harness, string, bool)
		name string
	}{
		{name: "claim", run: runCancellationClaimOrdering},
		{name: "heartbeat", run: runCancellationHeartbeatOrdering},
		{name: "retry scheduling", run: runCancellationRetryOrdering},
		{name: "retry promotion", run: runCancellationPromotionOrdering},
		{name: "lease recovery", run: runCancellationRecoveryOrdering},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			for _, cancelFirst := range []bool{true, false} {
				name := "operation-before-cancel"
				if cancelFirst {
					name = "cancel-before-operation"
				}

				t.Run(name, func(t *testing.T) {
					testCase.run(t, harness, testCase.name+"-"+name, cancelFirst)
				})
			}
		})
	}
}

func runCancellationClaimOrdering(t *testing.T, harness Harness, name string, cancelFirst bool) {
	t.Helper()

	ordering := openCancellationOrdering(t, harness, name)
	opened, plan, second := ordering.opened, ordering.plan, ordering.second

	if cancelFirst {
		cancelRun(t, second, plan.Run.ID)
		before := mustNodeSnapshot(t, opened.backend, plan.Run.ID)
		claim, claimed, err := claimAny(t.Context(), opened.backend, workerA)
		requireNotClaimed(t, claim, claimed, err)
		requireNodeSnapshotUnchanged(t, opened.backend, plan.Run.ID, &before, "claim after cancellation")

		return
	}

	claim := mustClaim(t, opened.backend, workerA)
	before := mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)
	cancelRun(t, second, plan.Run.ID)

	if before.State != storage.NodeRunning {
		t.Fatalf("claimed node state = %q, want %q", before.State, storage.NodeRunning)
	}

	assertCanceledNode(t, opened.backend, plan.Run.ID)
}

func runCancellationHeartbeatOrdering(t *testing.T, harness Harness, name string, cancelFirst bool) {
	t.Helper()

	ordering := openCancellationOrdering(t, harness, name)
	opened, plan, second := ordering.opened, ordering.plan, ordering.second

	claim := mustClaim(t, opened.backend, workerA)
	if cancelFirst {
		cancelRun(t, second, plan.Run.ID)
		before := mustNodeSnapshot(t, opened.backend, plan.Run.ID)

		accepted, remaining, err := opened.backend.HeartbeatNode(
			t.Context(), claim.RunID, claim.NodeID, claim.Lease, heartbeatExtension,
		)
		if err != nil || accepted || remaining != 0 {
			t.Fatalf("heartbeat after cancellation: accepted=%v remaining=%s err=%v", accepted, remaining, err)
		}

		requireNodeSnapshotUnchanged(t, opened.backend, plan.Run.ID, &before, "heartbeat after cancellation")

		return
	}

	accepted, remaining, err := opened.backend.HeartbeatNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, heartbeatExtension,
	)
	requireHeartbeat(t, accepted, remaining, claim.Lease.Remaining, err)
	cancelRun(t, second, plan.Run.ID)
	assertCanceledNode(t, opened.backend, plan.Run.ID)
}

func runCancellationRetryOrdering(t *testing.T, harness Harness, name string, cancelFirst bool) {
	t.Helper()

	ordering := openCancellationOrdering(t, harness, name)
	opened, plan, second := ordering.opened, ordering.plan, ordering.second

	claim := mustClaim(t, opened.backend, workerA)
	if cancelFirst {
		cancelRun(t, second, plan.Run.ID)
		before := mustNodeSnapshot(t, opened.backend, plan.Run.ID)
		accepted, err := opened.backend.RetryNode(
			t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"late retry"`), time.Hour,
		)
		requireRejected(t, retryAfterCancellation, accepted, err)
		requireNodeSnapshotUnchanged(t, opened.backend, plan.Run.ID, &before, retryAfterCancellation)

		return
	}

	accepted, err := opened.backend.RetryNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"retry"`), time.Hour,
	)
	requireAccepted(t, "retry before cancellation", accepted, err)
	cancelRun(t, second, plan.Run.ID)
	assertCanceledNode(t, opened.backend, plan.Run.ID)
}

func runCancellationPromotionOrdering(t *testing.T, harness Harness, name string, cancelFirst bool) {
	t.Helper()

	ordering := openCancellationOrdering(t, harness, name)
	opened, plan, second := ordering.opened, ordering.plan, ordering.second
	claim := mustClaim(t, opened.backend, workerA)
	accepted, err := opened.backend.RetryNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"retry"`), 0,
	)
	requireAccepted(t, "schedule retry for promotion", accepted, err)

	if cancelFirst {
		cancelRun(t, second, plan.Run.ID)
		before := mustNodeSnapshot(t, opened.backend, plan.Run.ID)

		promoted, promoteErr := opened.backend.PromoteRetries(t.Context())
		if promoteErr != nil || promoted != 0 {
			t.Fatalf("promotion after cancellation: count=%d err=%v", promoted, promoteErr)
		}

		requireNodeSnapshotUnchanged(t, opened.backend, plan.Run.ID, &before, "promotion after cancellation")

		return
	}

	promoted, promoteErr := opened.backend.PromoteRetries(t.Context())
	requireSingleCount(t, "promotion before cancellation", promoted, promoteErr)
	cancelRun(t, second, plan.Run.ID)
	assertCanceledNode(t, opened.backend, plan.Run.ID)
}

func runCancellationRecoveryOrdering(t *testing.T, harness Harness, name string, cancelFirst bool) {
	t.Helper()

	ordering := openCancellationOrdering(t, harness, name)
	opened, plan, second := ordering.opened, ordering.plan, ordering.second

	claim := mustClaim(t, opened.backend, workerA)
	if err := harness.ExpireLease(t.Context(), opened.database, claim.RunID, claim.NodeID); err != nil {
		t.Fatal(err)
	}

	if cancelFirst {
		cancelRun(t, second, plan.Run.ID)
		before := mustNodeSnapshot(t, opened.backend, plan.Run.ID)

		recovered, err := opened.backend.RecoverExpiredLeases(t.Context())
		if err != nil || recovered != 0 {
			t.Fatalf("recovery after cancellation: count=%d err=%v", recovered, err)
		}

		requireNodeSnapshotUnchanged(t, opened.backend, plan.Run.ID, &before, "recovery after cancellation")

		return
	}

	recovered, err := opened.backend.RecoverExpiredLeases(t.Context())
	requireSingleCount(t, "recovery before cancellation", recovered, err)
	cancelRun(t, second, plan.Run.ID)
	assertCanceledNode(t, opened.backend, plan.Run.ID)
}

type cancellationOrdering struct {
	second storage.Backend
	plan   *storage.RunPlan
	opened openedStore
}

func openCancellationOrdering(
	t *testing.T,
	harness Harness,
	name string,
) cancellationOrdering {
	t.Helper()

	opened := openStore(t, harness, "cancellation-ordering-"+name)

	plan := singleNodePlan(storage.RunID("conformance-cancellation-"+name), name)
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	second, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	return cancellationOrdering{opened: opened, plan: &plan, second: second}
}

func cancelRun(t *testing.T, backend storage.Backend, runID storage.RunID) {
	t.Helper()

	outcome, err := backend.CancelRun(t.Context(), runID)
	requireCancellationOutcome(t, outcome, err, storage.CancellationCanceled)
}

type nodeSnapshot struct {
	report storage.NodeReport
}

func mustNodeSnapshot(t *testing.T, backend storage.Backend, runID storage.RunID) nodeSnapshot {
	t.Helper()

	return nodeSnapshot{report: mustFindNode(t, backend, runID, conformanceNodeID)}
}

func requireNodeSnapshotUnchanged(
	t *testing.T,
	backend storage.Backend,
	runID storage.RunID,
	before *nodeSnapshot,
	operation string,
) {
	t.Helper()

	after := mustNodeSnapshot(t, backend, runID)
	if !reflect.DeepEqual(*before, after) {
		t.Fatalf("%s mutated node:\nbefore=%#v\nafter=%#v", operation, *before, after)
	}
}

func assertCanceledNode(t *testing.T, backend storage.Backend, runID storage.RunID) {
	t.Helper()

	node := mustFindNode(t, backend, runID, conformanceNodeID)
	if node.State != storage.NodeCanceled || node.Reason != storage.ReasonCanceledByRequest ||
		node.CurrentLease != nil {
		t.Fatalf("canceled node = %#v", node)
	}
}

func runConcurrentCancellation(t *testing.T, harness Harness) {
	t.Helper()

	const testName = "concurrent-cancellation"

	opened := openStore(t, harness, testName)

	plan := singleNodePlan(testName, testName)
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	second, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	const concurrentCancellations = 2

	type cancelResult struct {
		err     error
		outcome storage.CancellationOutcome
	}

	start := make(chan struct{})
	results := make(chan cancelResult, concurrentCancellations)

	for _, backend := range []storage.Backend{opened.backend, second} {
		go func(backend storage.Backend) {
			<-start

			outcome, cancelErr := backend.CancelRun(t.Context(), plan.Run.ID)
			results <- cancelResult{outcome: outcome, err: cancelErr}
		}(backend)
	}

	close(start)

	counts := map[storage.CancellationOutcome]int{}

	for range concurrentCancellations {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}

		counts[result.outcome]++
	}

	if counts[storage.CancellationCanceled] != 1 || counts[storage.CancellationAlreadyCanceled] != 1 {
		t.Fatalf("concurrent cancellation outcomes = %#v", counts)
	}
}

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
