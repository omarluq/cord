package conformance

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

type openedStore struct {
	database *sql.DB
	backend  storage.Backend
}

func openStore(t *testing.T, harness Harness, name string) openedStore {
	t.Helper()

	database := harness.Open(t, name)
	if err := harness.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}

	backend, err := harness.NewBackend(database)
	if err != nil {
		t.Fatal(err)
	}

	return openedStore{database: database, backend: backend}
}

func mustClaim(t *testing.T, store storage.Backend, owner string) *storage.Claim {
	t.Helper()

	claim, claimed, err := store.ClaimReadyNodeForFunctions(
		t.Context(), owner, time.Minute, conformanceRegistrations())
	if err != nil || !claimed || claim == nil {
		t.Fatalf("claim node: claim=%#v claimed=%v err=%v", claim, claimed, err)
	}

	return claim
}

func claimAny(ctx context.Context, backend storage.Backend, owner string) (*storage.Claim, bool, error) {
	claim, claimed, err := backend.ClaimReadyNodeForFunctions(
		ctx, owner, time.Minute, conformanceRegistrations(),
	)
	if err != nil {
		return claim, claimed, fmt.Errorf("claim ready node: %w", err)
	}

	return claim, claimed, nil
}

func conformanceRegistrations() []storage.FunctionRegistration {
	return []storage.FunctionRegistration{
		{Key: stepFunctionKey, Signature: "signature"},
		{Key: leftFunctionKey, Signature: "left"},
		{Key: rightFunctionKey, Signature: "right"},
		{Key: joinFunctionKey, Signature: "join"},
		{Key: completedNodeName, Signature: "completed-signature"},
		{Key: runningNodeName, Signature: "running-signature"},
		{Key: readyNodeName, Signature: "ready-signature"},
		{Key: retryingNodeName, Signature: "retry-signature"},
		{Key: pendingNodeName, Signature: "pending-signature"},
		{Key: terminalNodeName, Signature: "terminal-signature"},
	}
}

func requireAccepted(t *testing.T, operation string, accepted bool, err error) {
	t.Helper()

	if err != nil || !accepted {
		t.Fatalf("%s: accepted=%v err=%v", operation, accepted, err)
	}
}

func requireRejected(t *testing.T, operation string, accepted bool, err error) {
	t.Helper()

	if err != nil || accepted {
		t.Fatalf("%s: accepted=%v err=%v", operation, accepted, err)
	}
}

func requireRunResult(t *testing.T, result *storage.RunResult, status storage.RunStatus, output, runError []byte) {
	t.Helper()

	if result.Status != status || string(result.Output) != string(output) || string(result.Error) != string(runError) {
		t.Fatalf("run result = %#v, want status=%s output=%q error=%q", result, status, output, runError)
	}
}

func requireRunIdentity(t *testing.T, result *storage.RunResult, plan *storage.RunPlan) {
	t.Helper()

	var terminalSignature string

	for index := range plan.Nodes {
		if plan.Nodes[index].ID == plan.Run.TerminalNodeID {
			terminalSignature = plan.Nodes[index].SignatureHash

			break
		}
	}

	if result.WorkflowName != plan.Run.WorkflowName ||
		result.DefinitionHash != plan.Run.DefinitionHash ||
		result.TerminalSignatureHash != terminalSignature ||
		result.MaxAttempts != plan.Run.MaxAttempts ||
		result.RetryBaseDelay != plan.Run.RetryBaseDelay ||
		result.RetryMaxDelay != plan.Run.RetryMaxDelay ||
		result.RetryPolicyVersion != plan.Run.RetryPolicyVersion {
		t.Fatalf("run result identity = %#v, want run identity and retry metadata from %#v",
			result, plan.Run)
	}
}

func requireNodeIDs(t *testing.T, first, second *storage.Claim, firstID, secondID storage.NodeID) {
	t.Helper()

	if first.NodeID != firstID || second.NodeID != secondID {
		t.Fatalf("root claims = %q, %q, want %q, %q", first.NodeID, second.NodeID, firstID, secondID)
	}
}

func requireNotClaimed(t *testing.T, claim *storage.Claim, claimed bool, err error) {
	t.Helper()

	if err != nil || claimed {
		t.Fatalf("unexpected claim: claim=%#v claimed=%v err=%v", claim, claimed, err)
	}
}

func requireHeartbeat(t *testing.T, accepted bool, remaining, previous time.Duration, err error) {
	t.Helper()

	if err != nil || !accepted || remaining <= 0 || remaining < previous/2 {
		t.Fatalf("heartbeat: accepted=%v remaining=%v previous=%v err=%v", accepted, remaining, previous, err)
	}
}

func requireSingleCount(t *testing.T, operation string, got int64, err error) {
	t.Helper()

	if err != nil || got != 1 {
		t.Fatalf("%s: count=%d want=1 err=%v", operation, got, err)
	}
}

func requireRenewedClaim(t *testing.T, current, previous *storage.Claim) {
	t.Helper()

	if current.Attempt != previous.Attempt+1 || current.Lease.Generation <= previous.Lease.Generation {
		t.Fatalf("renewed claim = %#v, previous=%#v", current, previous)
	}
}
