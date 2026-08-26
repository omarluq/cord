package conformance

import (
	"reflect"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

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
