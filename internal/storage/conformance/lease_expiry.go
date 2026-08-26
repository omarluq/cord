package conformance

import (
	"encoding/json"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

func runFinalAttemptLeaseExpiry(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "final-attempt-expiry")
	plan := joinPlan("conformance-final-attempt-expiry")

	plan.Run.MaxAttempts = 1
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	exhausted := mustClaim(t, opened.backend, workerA)

	sibling := mustClaim(t, opened.backend, workerB)
	if err := harness.ExpireLease(t.Context(), opened.database, exhausted.RunID, exhausted.NodeID); err != nil {
		t.Fatal(err)
	}

	recovered, err := opened.backend.RecoverExpiredLeases(t.Context())
	requireSingleCount(t, "recover exhausted lease", recovered, err)

	result, err := opened.backend.GetRunResult(t.Context(), plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != storage.RunFailed {
		t.Fatalf("exhausted run status = %q, want %q", result.Status, storage.RunFailed)
	}

	requireLeaseExpiryFailure(t, result.Error, exhausted)
	requireFinalAttemptNodeStates(t, harness, opened, exhausted, sibling)
	requireFinalAttemptFences(t, opened.backend, exhausted, sibling)
}

func requireFinalAttemptNodeStates(
	t *testing.T,
	harness Harness,
	opened openedStore,
	exhausted, sibling *storage.Claim,
) {
	t.Helper()

	states, err := harness.LoadNodeStates(t.Context(), opened.database, exhausted.RunID)
	if err != nil {
		t.Fatal(err)
	}

	exhaustedState := states[exhausted.NodeID]
	if exhaustedState.Status != storage.NodeFailed || exhaustedState.LeaseOwner != "" ||
		exhaustedState.HasLeaseExpiry || exhaustedState.LeaseGeneration <= exhausted.Lease.Generation {
		t.Fatalf("exhausted node state = %#v, claim=%#v", exhaustedState, exhausted)
	}

	requireLeaseExpiryFailure(t, exhaustedState.Error, exhausted)

	siblingState := states[sibling.NodeID]
	if siblingState.Status != storage.NodeCanceled || siblingState.LeaseOwner != "" || siblingState.HasLeaseExpiry {
		t.Fatalf("sibling node state = %#v", siblingState)
	}
}

func requireFinalAttemptFences(t *testing.T, backend storage.Backend, exhausted, sibling *storage.Claim) {
	t.Helper()

	accepted, err := backend.CompleteNode(
		t.Context(), exhausted.RunID, exhausted.NodeID, exhausted.Lease, []byte(`"late"`),
	)
	requireRejected(t, "expired exhausted completion", accepted, err)

	accepted, err = backend.CompleteNode(
		t.Context(), sibling.RunID, sibling.NodeID, sibling.Lease, []byte(`"late sibling"`),
	)
	requireRejected(t, "canceled sibling completion", accepted, err)
}

func runConcurrentFinalAttemptRecovery(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "concurrent-final-attempt-expiry")
	plan := singleNodePlan("conformance-concurrent-final-expiry", "concurrent-final-expiry")

	plan.Run.MaxAttempts = 1
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, opened.backend, workerA)
	if err := harness.ExpireLease(t.Context(), opened.database, claim.RunID, claim.NodeID); err != nil {
		t.Fatal(err)
	}

	restarted, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	type recoveryResult struct {
		err   error
		count int64
	}

	const concurrentRecoveries = 2

	start := make(chan struct{})
	results := make(chan recoveryResult, concurrentRecoveries)

	for _, backend := range []storage.Backend{opened.backend, restarted} {
		go func(store storage.Backend) {
			<-start

			count, recoverErr := store.RecoverExpiredLeases(t.Context())
			results <- recoveryResult{count: count, err: recoverErr}
		}(backend)
	}

	close(start)

	var total int64

	for range concurrentRecoveries {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}

		total += result.count
	}

	if total != 1 {
		t.Fatalf("concurrent recovered count = %d, want 1", total)
	}

	runResult, err := restarted.GetRunResult(t.Context(), plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if runResult.Status != storage.RunFailed {
		t.Fatalf("concurrently recovered run status = %q, want %q", runResult.Status, storage.RunFailed)
	}

	requireLeaseExpiryFailure(t, runResult.Error, claim)
}

func requireLeaseExpiryFailure(t *testing.T, payload storage.EncodedPayload, claim *storage.Claim) {
	t.Helper()

	var failure struct {
		Message     string `json:"message"`
		NodeID      string `json:"node_id"`
		FunctionKey string `json:"function_key"`
		Attempt     int    `json:"attempt"`
		Retryable   bool   `json:"retryable"`
	}
	if err := json.Unmarshal(payload, &failure); err != nil {
		t.Fatalf("decode lease-expiry failure %q: %v", payload, err)
	}

	if failure.Message == "" || failure.NodeID != string(claim.NodeID) ||
		failure.FunctionKey != claim.FunctionKey || failure.Attempt != claim.Attempt || failure.Retryable {
		t.Fatalf("lease-expiry failure = %#v, claim=%#v", failure, claim)
	}
}
