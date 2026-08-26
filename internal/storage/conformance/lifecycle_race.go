package conformance

import (
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

func runCompletionCancellationRace(t *testing.T, harness Harness) {
	t.Helper()

	runTerminalCancellationRace(t, harness, "completion-cancellation-race", storage.RunCompleted,
		func(backend storage.Backend, claim *storage.Claim) (bool, error) {
			return backend.CompleteNode(
				t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"completed"`),
			)
		})
}

func runFailureCancellationRace(t *testing.T, harness Harness) {
	t.Helper()

	runTerminalCancellationRace(t, harness, "failure-cancellation-race", storage.RunFailed,
		func(backend storage.Backend, claim *storage.Claim) (bool, error) {
			return backend.FailNode(
				t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`{"message":"failed"}`),
				storage.ReasonFailureNonRetryable,
			)
		})
}

func runTerminalCancellationRace(
	t *testing.T,
	harness Harness,
	name string,
	transitionStatus storage.RunStatus,
	transition func(storage.Backend, *storage.Claim) (bool, error),
) {
	t.Helper()

	opened := openStore(t, harness, name)

	plan := singleNodePlan(storage.RunID("conformance-"+name), name)
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, opened.backend, "transition-worker")

	second, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	type transitionResult struct {
		err      error
		accepted bool
	}

	type cancellationResult struct {
		err     error
		outcome storage.CancellationOutcome
	}

	start := make(chan struct{})
	transitionResults := make(chan transitionResult, 1)
	cancellationResults := make(chan cancellationResult, 1)

	go func() {
		<-start

		accepted, transitionErr := transition(opened.backend, claim)
		transitionResults <- transitionResult{accepted: accepted, err: transitionErr}
	}()
	go func() {
		<-start

		outcome, cancelErr := second.CancelRun(t.Context(), plan.Run.ID)
		cancellationResults <- cancellationResult{outcome: outcome, err: cancelErr}
	}()

	close(start)

	transitionResultValue := <-transitionResults

	cancellationResultValue := <-cancellationResults
	if transitionResultValue.err != nil || cancellationResultValue.err != nil {
		t.Fatalf("terminal/cancellation race errors: transition=%v cancellation=%v",
			transitionResultValue.err, cancellationResultValue.err)
	}

	result, err := opened.backend.GetRunResult(t.Context(), plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	node := mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)
	report := mustInspectRun(t, opened.backend, plan.Run.ID)

	requireTerminalRaceOutcome(t, terminalRaceOutcome{
		result:             &result,
		node:               &node,
		report:             &report,
		cancellation:       cancellationResultValue.outcome,
		transitionAccepted: transitionResultValue.accepted,
	}, terminalRaceExpectation{
		status: transitionStatus,
		owner:  claim.Lease.Owner,
	})
}

func requireTerminalRaceOutcome(
	t *testing.T,
	outcome terminalRaceOutcome,
	expectation terminalRaceExpectation,
) {
	t.Helper()

	if outcome.transitionAccepted {
		requireTransitionWonRace(
			t, outcome.cancellation, outcome.result, outcome.node, outcome.report,
			expectation.status, expectation.owner,
		)

		return
	}

	requireCancellationWonRace(
		t, outcome.cancellation, outcome.result, outcome.node, outcome.report,
	)
}

func requireTransitionWonRace(
	t *testing.T,
	outcome storage.CancellationOutcome,
	result *storage.RunResult,
	node *storage.NodeReport,
	report *storage.RunReport,
	status storage.RunStatus,
	owner string,
) {
	t.Helper()

	validRun := outcome == storage.CancellationFinished && result.Status == status
	validNode := node.State == nodeStatusForRun(status)

	validRunner := report.TerminalRunnerID != nil && string(*report.TerminalRunnerID) == owner
	if !validRun || !validNode || !validRunner {
		t.Fatalf("transition-winning race: outcome=%q result=%#v node=%#v report=%#v",
			outcome, result, node, report)
	}
}

func requireCancellationWonRace(
	t *testing.T,
	outcome storage.CancellationOutcome,
	result *storage.RunResult,
	node *storage.NodeReport,
	report *storage.RunReport,
) {
	t.Helper()

	validRun := outcome == storage.CancellationCanceled && result.Status == storage.RunCanceled
	validNode := node.State == storage.NodeCanceled && node.Reason == storage.ReasonCanceledByRequest

	if !validRun || !validNode || report.TerminalRunnerID != nil {
		t.Fatalf("cancellation-winning race: outcome=%q result=%#v node=%#v report=%#v",
			outcome, result, node, report)
	}
}

func nodeStatusForRun(status storage.RunStatus) storage.NodeStatus {
	if status == storage.RunCompleted {
		return storage.NodeCompleted
	}

	return storage.NodeFailed
}
