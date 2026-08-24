package conformance

import (
	"reflect"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

type concurrentClaimResult struct {
	claim   *storage.Claim
	err     error
	owner   string
	claimed bool
}

func runConcurrentClaimWinnerMetadata(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "concurrent-claim-metadata")

	plan := singleNodePlan("conformance-concurrent-claim-metadata", "concurrent-claim-metadata")
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	second, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	backends := []struct {
		backend storage.Backend
		owner   string
	}{
		{backend: opened.backend, owner: workerA},
		{backend: second, owner: workerB},
	}
	start := make(chan struct{})

	results := make(chan concurrentClaimResult, len(backends))
	for _, candidate := range backends {
		go func() {
			<-start

			claim, claimed, claimErr := claimAny(t.Context(), candidate.backend, candidate.owner)
			results <- concurrentClaimResult{
				claim: claim, claimed: claimed, err: claimErr, owner: candidate.owner,
			}
		}()
	}

	close(start)

	winner := requireSingleClaimWinner(t, results, len(backends))

	node := mustFindNode(t, opened.backend, plan.Run.ID, conformanceNodeID)
	requireWinningClaimMetadata(t, winner, &node)

	report := mustInspectRun(t, opened.backend, plan.Run.ID)
	if report.FirstStartedAt == nil {
		t.Fatalf("concurrently claimed run report = %#v", report)
	}
}

func requireWinningClaimMetadata(
	t *testing.T,
	winner *concurrentClaimResult,
	node *storage.NodeReport,
) {
	t.Helper()

	if winner.claim.Lease.Owner != winner.owner || node.State != storage.NodeRunning {
		t.Fatalf("winner=%#v, persisted node=%#v", winner, node)
	}

	if node.RunnerID == nil || string(*node.RunnerID) != winner.owner {
		t.Fatalf("winner=%#v, persisted node=%#v", winner, node)
	}

	if node.CurrentLease == nil || string(node.CurrentLease.RunnerID) != winner.owner {
		t.Fatalf("winner=%#v, persisted node=%#v", winner, node)
	}

	if node.CurrentLease.Generation != winner.claim.Lease.Generation || node.Attempt != winner.claim.Attempt {
		t.Fatalf("winner=%#v, persisted node=%#v", winner, node)
	}
}

func requireSingleClaimWinner(
	t *testing.T,
	results <-chan concurrentClaimResult,
	resultCount int,
) *concurrentClaimResult {
	t.Helper()

	var winner *concurrentClaimResult

	for range resultCount {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}

		if !result.claimed {
			if result.claim != nil {
				t.Fatalf("losing claim = %#v, want nil", result.claim)
			}

			continue
		}

		if result.claim == nil {
			t.Fatal("winning claim is nil")
		}

		if winner != nil {
			t.Fatalf("multiple concurrent claim winners: %#v and %#v", winner, result)
		}

		winner = &result
	}

	if winner == nil {
		t.Fatal("concurrent claim had no winner")
	}

	return winner
}

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

	requireTerminalRaceOutcome(
		t, transitionResultValue.accepted, cancellationResultValue.outcome,
		&result, &node, &report, transitionStatus, claim.Lease.Owner,
	)
}

func requireTerminalRaceOutcome(
	t *testing.T,
	transitionAccepted bool,
	cancellationOutcome storage.CancellationOutcome,
	result *storage.RunResult,
	node *storage.NodeReport,
	report *storage.RunReport,
	transitionStatus storage.RunStatus,
	owner string,
) {
	t.Helper()

	if transitionAccepted {
		requireTransitionWonRace(t, cancellationOutcome, result, node, report, transitionStatus, owner)

		return
	}

	requireCancellationWonRace(t, cancellationOutcome, result, node, report)
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

func runTerminalLifecycleAbsorption(t *testing.T, harness Harness) {
	t.Helper()

	tests := []struct {
		terminalize func(*testing.T, storage.Backend, *storage.Claim)
		name        string
	}{
		{
			name: "completed",
			terminalize: func(t *testing.T, backend storage.Backend, claim *storage.Claim) {
				t.Helper()
				accepted, err := backend.CompleteNode(
					t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"completed"`),
				)
				requireAccepted(t, "terminal completion", accepted, err)
			},
		},
		{
			name: "failed",
			terminalize: func(t *testing.T, backend storage.Backend, claim *storage.Claim) {
				t.Helper()
				accepted, err := backend.FailNode(
					t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`{"message":"failed"}`),
					storage.ReasonFailureNonRetryable,
				)
				requireAccepted(t, "terminal failure", accepted, err)
			},
		},
		{
			name: "canceled",
			terminalize: func(t *testing.T, backend storage.Backend, claim *storage.Claim) {
				t.Helper()

				outcome, err := backend.CancelRun(t.Context(), claim.RunID)
				if err != nil || outcome != storage.CancellationCanceled {
					t.Fatalf("terminal cancellation: outcome=%q err=%v", outcome, err)
				}
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			opened := openStore(t, harness, "terminal-absorption-"+testCase.name)

			plan := singleNodePlan(storage.RunID("conformance-terminal-"+testCase.name), testCase.name)
			if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
				t.Fatal(err)
			}

			claim := mustClaim(t, opened.backend, workerA)
			testCase.terminalize(t, opened.backend, claim)
			beforeRun := mustInspectRun(t, opened.backend, plan.Run.ID)
			beforeNode := mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)

			assertRejectedTerminalOperations(t, opened.backend, claim)

			afterRun := mustInspectRun(t, opened.backend, plan.Run.ID)
			afterNode := mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)

			if !reflect.DeepEqual(beforeRun, afterRun) || !reflect.DeepEqual(beforeNode, afterNode) {
				t.Fatalf("terminal lifecycle metadata changed:\n"+
					"run before=%#v\nrun after=%#v\nnode before=%#v\nnode after=%#v",
					beforeRun, afterRun, beforeNode, afterNode)
			}
		})
	}
}

func assertRejectedTerminalOperations(t *testing.T, backend storage.Backend, claim *storage.Claim) {
	t.Helper()

	accepted, err := backend.CompleteNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"late"`),
	)
	requireRejected(t, "late completion", accepted, err)

	accepted, err = backend.FailNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`{"message":"late"}`),
		storage.ReasonFailureAttemptsExhausted,
	)
	requireRejected(t, "late failure", accepted, err)

	accepted, remaining, err := backend.HeartbeatNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, heartbeatExtension,
	)
	if err != nil || accepted || remaining != 0 {
		t.Fatalf("late heartbeat: accepted=%v remaining=%s err=%v", accepted, remaining, err)
	}

	outcome, err := backend.CancelRun(t.Context(), claim.RunID)
	if err != nil || (outcome != storage.CancellationFinished &&
		outcome != storage.CancellationAlreadyCanceled) {
		t.Fatalf("late cancellation: outcome=%q err=%v", outcome, err)
	}
}

func runStaleHeartbeatMetadataFence(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "stale-heartbeat-metadata-fence")

	plan := singleNodePlan("conformance-stale-heartbeat-metadata-fence", "stale-heartbeat-metadata-fence")
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	stale := mustClaim(t, opened.backend, workerA)
	if err := harness.ExpireLease(t.Context(), opened.database, stale.RunID, stale.NodeID); err != nil {
		t.Fatal(err)
	}

	recovered, err := opened.backend.RecoverExpiredLeases(t.Context())
	requireSingleCount(t, "recover stale heartbeat lease", recovered, err)
	current := mustClaim(t, opened.backend, workerB)
	requireRenewedClaim(t, current, stale)

	before := mustFindNode(t, opened.backend, plan.Run.ID, current.NodeID)

	accepted, remaining, err := opened.backend.HeartbeatNode(
		t.Context(), stale.RunID, stale.NodeID, stale.Lease, heartbeatExtension,
	)
	if err != nil || accepted || remaining != 0 {
		t.Fatalf("stale heartbeat: accepted=%v remaining=%s err=%v", accepted, remaining, err)
	}

	after := mustFindNode(t, opened.backend, plan.Run.ID, current.NodeID)
	requireUnchangedWinnerMetadata(t, &before, &after, current)
}

func requireUnchangedWinnerMetadata(
	t *testing.T,
	before, after *storage.NodeReport,
	claim *storage.Claim,
) {
	t.Helper()

	if !reflect.DeepEqual(after, before) {
		t.Fatalf("stale heartbeat mutated winner metadata:\nbefore=%#v\nafter=%#v", before, after)
	}

	validRunner := after.RunnerID != nil && string(*after.RunnerID) == workerB

	validLease := after.CurrentLease != nil &&
		string(after.CurrentLease.RunnerID) == workerB &&
		after.CurrentLease.Generation == claim.Lease.Generation
	if !validRunner || !validLease {
		t.Fatalf("current claim metadata = %#v, claim=%#v", after, claim)
	}
}
