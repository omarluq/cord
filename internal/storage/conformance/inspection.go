package conformance

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func runInspectionSnapshots(t *testing.T, harness Harness) {
	t.Helper()

	const rootCount = 2

	opened := openStore(t, harness, "inspection-snapshots")

	plan := joinPlan("conformance-inspection")
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	report := mustInspectRun(t, opened.backend, plan.Run.ID)
	requireInspectionRun(t, &report, plan.Run.ID, storage.RunRunning, "", storage.NodeStateCounts{
		Pending: 1, Ready: rootCount, Running: 0, RetryWait: 0, Completed: 0, Failed: 0, Canceled: 0,
	})
	requireInitialRunReport(t, &report, plan.Run.WorkflowName)

	claim := mustClaim(t, opened.backend, workerA)
	runningRun := mustInspectRun(t, opened.backend, plan.Run.ID)
	requireInspectionRun(t, &runningRun, plan.Run.ID, storage.RunRunning, "", storage.NodeStateCounts{
		Pending: 1, Ready: 1, Running: 1, RetryWait: 0, Completed: 0, Failed: 0, Canceled: 0,
	})
	requireStartedRunReport(t, &runningRun)

	runningNode := mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)
	requireRunningNodeReport(t, &runningNode, claim)

	accepted, err := opened.backend.CompleteNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"done"`),
	)
	requireAccepted(t, "complete inspected node", accepted, err)

	completedNode := mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)
	requireCompletedNodeReport(t, &completedNode)

	outcome, err := opened.backend.CancelRun(t.Context(), plan.Run.ID)
	requireCancellationOutcome(t, outcome, err, storage.CancellationCanceled)

	canceled := mustInspectRun(t, opened.backend, plan.Run.ID)
	requireInspectionRun(t, &canceled, plan.Run.ID, storage.RunCanceled,
		storage.ReasonCanceledByRequest, storage.NodeStateCounts{
			Pending: 0, Ready: 0, Running: 0, RetryWait: 0, Completed: 1, Failed: 0, Canceled: rootCount,
		})
	requireCanceledRunReport(t, &canceled)

	_, err = opened.backend.InspectRun(t.Context(), "missing-inspection-run")
	if !errors.Is(err, storage.ErrRunNotFound) {
		t.Fatalf("InspectRun(missing) error = %v, want %v", err, storage.ErrRunNotFound)
	}
}

func runInspectionIsReadOnly(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "inspection-read-only")

	plan := singleNodePlan("conformance-inspection-read-only", "inspection-read-only")
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, opened.backend, workerA)
	accepted, err := opened.backend.RetryNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"later"`), time.Hour,
	)
	requireAccepted(t, "schedule inspected retry", accepted, err)

	beforeRun := mustInspectRun(t, opened.backend, plan.Run.ID)

	beforeNode := mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)
	for range 2 {
		_ = mustInspectRun(t, opened.backend, plan.Run.ID)
		_ = mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)
	}

	afterRun := mustInspectRun(t, opened.backend, plan.Run.ID)

	afterNode := mustFindNode(t, opened.backend, plan.Run.ID, claim.NodeID)
	if !reflect.DeepEqual(beforeRun, afterRun) || !reflect.DeepEqual(beforeNode, afterNode) {
		t.Fatalf("inspection mutated lifecycle state:\nbefore run=%#v\nafter run=%#v\nbefore node=%#v\nafter node=%#v",
			beforeRun, afterRun, beforeNode, afterNode)
	}

	promoted, err := opened.backend.PromoteRetries(t.Context())
	if err != nil || promoted != 0 {
		t.Fatalf("inspection promoted future retry: count=%d err=%v", promoted, err)
	}

	claimAfterRead, claimed, err := claimAny(t.Context(), opened.backend, workerB)
	requireNotClaimed(t, claimAfterRead, claimed, err)
}
