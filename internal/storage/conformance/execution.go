package conformance

import (
	"fmt"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

func runJoinOrder(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "join")
	store := opened.backend

	plan := joinPlan("conformance-join")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, workerA)

	second := mustClaim(t, store, workerB)
	requireNodeIDs(t, first, second, leftNodeID, rightNodeID)

	accepted, err := store.CompleteNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"left"`))
	requireAccepted(t, "complete left", accepted, err)

	earlyClaim, earlyClaimed, earlyErr := claimAny(t.Context(), store, "worker-early")
	requireNotClaimed(t, earlyClaim, earlyClaimed, earlyErr)

	accepted, err = store.CompleteNode(t.Context(), second.RunID, second.NodeID, second.Lease, []byte(`"right"`))
	requireAccepted(t, "complete right", accepted, err)

	join := mustClaim(t, store, "worker-join")
	if join.NodeID != joinNodeID {
		t.Fatalf("third claim = %q, want join", join.NodeID)
	}

	inputs, err := store.LoadNodeInputs(t.Context(), join.RunID, join.NodeID)
	if err != nil {
		t.Fatal(err)
	}

	if fmt.Sprint(inputs) != fmt.Sprint([]storage.EncodedPayload{[]byte(`"right"`), []byte(`"left"`)}) {
		t.Fatalf("join inputs = %q, want parent order [right left]", inputs)
	}
}

func runClaimAndCompletionFence(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "claim-fence")
	store := opened.backend

	plan := singleNodePlan("conformance-claim", "claim")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, store, "winner")
	duplicate, claimed, err := claimAny(t.Context(), store, "loser")
	requireNotClaimed(t, duplicate, claimed, err)

	stale := claim.Lease
	stale.Generation--

	accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, stale, []byte(`"stale"`))
	requireRejected(t, "stale completion", accepted, err)

	accepted, err = store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"current"`))
	requireAccepted(t, "owned completion", accepted, err)
}

func runRetryAndPromotion(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "retry")
	store := opened.backend

	plan := singleNodePlan("conformance-retry", "retry")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, workerA)

	accepted, err := store.RetryNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"retry"`), 0)
	requireAccepted(t, "retry node", accepted, err)

	promoted, err := store.PromoteRetries(t.Context())
	requireSingleCount(t, "promote retries", promoted, err)

	second := mustClaim(t, store, workerB)
	if second.Attempt != 2 || second.Lease.Generation <= first.Lease.Generation {
		t.Fatalf("retry claim = %#v, first generation=%d", second, first.Lease.Generation)
	}
}

func runFailure(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "failure")
	store := opened.backend

	plan := joinPlan("conformance-failure")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, store, "worker")
	failure := []byte(`{"message":"permanent"}`)

	accepted, err := store.FailNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, failure,
		storage.ReasonFailureNonRetryable,
	)
	requireAccepted(t, "fail node", accepted, err)

	result, err := store.GetRunResult(t.Context(), claim.RunID)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != storage.RunFailed || string(result.Error) != string(failure) {
		t.Fatalf("failed result = %#v", result)
	}

	runReport := mustInspectRun(t, store, claim.RunID)
	if runReport.Reason != storage.ReasonFailureNonRetryable {
		t.Fatalf("failed run reason = %q, want %q", runReport.Reason, storage.ReasonFailureNonRetryable)
	}

	nodeReport := mustFindNode(t, store, claim.RunID, claim.NodeID)
	if nodeReport.Reason != storage.ReasonFailureNonRetryable {
		t.Fatalf("failed node reason = %q, want %q", nodeReport.Reason, storage.ReasonFailureNonRetryable)
	}

	if next, claimed, claimErr := claimAny(t.Context(), store, "other"); claimErr != nil || claimed {
		t.Fatalf("claim after failure = %#v, claimed=%v err=%v", next, claimed, claimErr)
	}
}

func runHeartbeatAndRecovery(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "lease")
	database, store := opened.database, opened.backend

	plan := singleNodePlan("conformance-lease", "lease")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, workerA)

	accepted, expiry, err := store.HeartbeatNode(
		t.Context(), first.RunID, first.NodeID, first.Lease, heartbeatExtension,
	)
	requireHeartbeat(t, accepted, expiry, first.Lease.Remaining, err)

	if expireErr := harness.ExpireLease(t.Context(), database, first.RunID, first.NodeID); expireErr != nil {
		t.Fatal(expireErr)
	}

	recovered, err := store.RecoverExpiredLeases(t.Context())
	requireSingleCount(t, "recover lease", recovered, err)

	second := mustClaim(t, store, workerB)
	requireRenewedClaim(t, second, first)

	accepted, err = store.CompleteNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"stale"`))
	requireRejected(t, "expired lease completion", accepted, err)
}
