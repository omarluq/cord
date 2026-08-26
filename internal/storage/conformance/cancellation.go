package conformance

import (
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
