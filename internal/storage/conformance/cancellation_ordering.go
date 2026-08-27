package conformance

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

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
		{name: "final attempt lease recovery", run: runCancellationFinalAttemptRecoveryOrdering},
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

func runCancellationFinalAttemptRecoveryOrdering(
	t *testing.T,
	harness Harness,
	name string,
	cancelFirst bool,
) {
	t.Helper()

	opened := openStore(t, harness, "cancellation-ordering-"+name)
	plan := joinPlan(storage.RunID("conformance-cancellation-" + name))

	plan.Run.MaxAttempts = 1
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	exhausted := mustClaim(t, opened.backend, workerA)
	sibling := mustClaim(t, opened.backend, workerB)

	if err := harness.ExpireLease(t.Context(), opened.database, exhausted.RunID, exhausted.NodeID); err != nil {
		t.Fatal(err)
	}

	second, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	if cancelFirst {
		cancelRun(t, second, plan.Run.ID)
		before := mustDurableRunSnapshot(t, harness, opened, plan.Run.ID)

		recovered, recoverErr := opened.backend.RecoverExpiredLeases(t.Context())
		if recoverErr != nil || recovered != 0 {
			t.Fatalf("final-attempt recovery after cancellation: count=%d err=%v", recovered, recoverErr)
		}

		requireDurableRunSnapshotUnchanged(
			t, harness, opened, plan.Run.ID, &before, "final-attempt recovery after cancellation",
		)
		requireCanceledJoinRun(t, opened.backend, plan.Run.ID)

		return
	}

	recovered, recoverErr := opened.backend.RecoverExpiredLeases(t.Context())
	requireSingleCount(t, "final-attempt recovery before cancellation", recovered, recoverErr)
	before := mustDurableRunSnapshot(t, harness, opened, plan.Run.ID)

	outcome, cancelErr := second.CancelRun(t.Context(), plan.Run.ID)
	requireCancellationOutcome(t, outcome, cancelErr, storage.CancellationFinished)
	requireDurableRunSnapshotUnchanged(
		t, harness, opened, plan.Run.ID, &before, "cancellation after final-attempt recovery",
	)
	requireFailedJoinRun(t, harness, opened, exhausted, sibling)
}

func requireCanceledJoinRun(t *testing.T, backend storage.Backend, runID storage.RunID) {
	t.Helper()

	const joinNodeCount = 3

	result, err := backend.GetRunResult(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}

	requireRunResult(t, &result, storage.RunCanceled, nil, nil)
	report := mustInspectRun(t, backend, runID)
	requireInspectionRun(t, &report, runID, storage.RunCanceled, storage.ReasonCanceledByRequest,
		storage.NodeStateCounts{
			Pending: 0, Ready: 0, Running: 0, RetryWait: 0, Completed: 0, Failed: 0, Canceled: joinNodeCount,
		})
}

func requireFailedJoinRun(
	t *testing.T,
	harness Harness,
	opened openedStore,
	exhausted, sibling *storage.Claim,
) {
	t.Helper()

	const canceledSiblingCount = 2

	result, err := opened.backend.GetRunResult(t.Context(), exhausted.RunID)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != storage.RunFailed {
		t.Fatalf("final-attempt recovery run status = %q, want %q", result.Status, storage.RunFailed)
	}

	requireLeaseExpiryFailure(t, result.Error, exhausted)
	requireFinalAttemptNodeStates(t, harness, opened, exhausted, sibling)
	report := mustInspectRun(t, opened.backend, exhausted.RunID)
	requireInspectionRun(t, &report, exhausted.RunID, storage.RunFailed,
		storage.ReasonFailureLeaseExpired, storage.NodeStateCounts{
			Pending: 0, Ready: 0, Running: 0, RetryWait: 0, Completed: 0, Failed: 1,
			Canceled: canceledSiblingCount,
		})
}
