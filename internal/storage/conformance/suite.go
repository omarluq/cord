// Package conformance verifies storage backend behavior.
package conformance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

type runCanceler interface {
	CancelRun(context.Context, storage.RunID) (bool, error)
}

// Harness supplies backend-specific database lifecycle operations to the common suite.
type Harness struct {
	// Open opens a test database identified by name.
	Open func(testing.TB, string) *sql.DB
	// Migrate brings a newly opened database to the current schema.
	Migrate func(context.Context, *sql.DB) error
	// NewBackend constructs a backend over an open, migrated database.
	NewBackend func(*sql.DB) (storage.Backend, error)
	// ExpireLease makes a claimed lease eligible for recovery.
	ExpireLease func(context.Context, *sql.DB, storage.RunID, storage.NodeID) error
	// DeleteRun deletes a run using the backend's native test mechanism.
	DeleteRun func(context.Context, *sql.DB, storage.RunID) error
}

const (
	conformanceNodeID  storage.NodeID = "node"
	leftNodeID         storage.NodeID = "left"
	rightNodeID        storage.NodeID = "right"
	joinNodeID         storage.NodeID = "join"
	heartbeatExtension                = 2 * time.Minute
	joinDependencies                  = 2
	workerA                           = "worker-a"
	workerB                           = "worker-b"
)

// Run executes Cord's backend-neutral behavioral storage conformance suite.
func Run(t *testing.T, harness Harness) {
	t.Helper()
	validateHarness(t, harness)

	tests := []struct {
		run  func(*testing.T, Harness)
		name string
	}{
		{name: "create and result", run: runCreateAndResult},
		{name: "join order and dependency release", run: runJoinOrder},
		{name: "claim uniqueness and completion fence", run: runClaimAndCompletionFence},
		{name: "retry and promotion", run: runRetryAndPromotion},
		{name: "failure", run: runFailure},
		{name: "cancellation", run: runCancellation},
		{name: "heartbeat and recovery", run: runHeartbeatAndRecovery},
		{name: "restart and resume", run: runRestartAndResume},
		{name: "migration idempotence", run: runMigrationIdempotence},
		{name: "run deletion", run: runRunDeletion},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) { testCase.run(t, harness) })
	}
}

func validateHarness(t *testing.T, harness Harness) {
	t.Helper()

	if harness.Open == nil || harness.Migrate == nil || harness.NewBackend == nil ||
		harness.ExpireLease == nil || harness.DeleteRun == nil {
		t.Fatal("conformance harness requires Open, Migrate, NewBackend, ExpireLease, and DeleteRun")
	}
}

func runCreateAndResult(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "create-result")
	store := opened.backend
	plan := singleNodePlan("conformance-create", "create")

	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	result, err := store.GetRunResult(t.Context(), plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	requireRunResult(t, result, storage.RunRunning, nil, nil)

	claim := mustClaim(t, store, "worker")

	const output = `"completed"`

	accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(output))
	requireAccepted(t, "complete node", accepted, err)

	result, err = store.GetRunResult(t.Context(), plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	requireRunResult(t, result, storage.RunCompleted, []byte(output), nil)

	_, err = store.GetRunResult(t.Context(), "missing-run")
	if !errors.Is(err, storage.ErrRunNotFound) {
		t.Fatalf("missing result error = %v, want %v", err, storage.ErrRunNotFound)
	}
}

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
	requireCount(t, "promote retries", promoted, 1, err)

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

	accepted, err := store.FailNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, failure)
	requireAccepted(t, "fail node", accepted, err)

	result, err := store.GetRunResult(t.Context(), claim.RunID)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != storage.RunFailed || string(result.Error) != string(failure) {
		t.Fatalf("failed result = %#v", result)
	}

	if next, claimed, claimErr := claimAny(t.Context(), store, "other"); claimErr != nil || claimed {
		t.Fatalf("claim after failure = %#v, claimed=%v err=%v", next, claimed, claimErr)
	}
}

func runCancellation(t *testing.T, harness Harness) {
	t.Helper()

	backend := openStore(t, harness, "cancel").backend

	canceler, ok := backend.(runCanceler)
	if !ok {
		t.Skip("backend does not support cancellation")
	}

	plan := joinPlan("conformance-cancel")
	if err := backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, backend, "worker")
	accepted, err := canceler.CancelRun(t.Context(), claim.RunID)
	requireAccepted(t, "cancel run", accepted, err)

	result, err := backend.GetRunResult(t.Context(), claim.RunID)
	if err != nil || result.Status != storage.RunCanceled {
		t.Fatalf("canceled result = %#v, err=%v", result, err)
	}

	accepted, err = backend.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"late"`))
	requireRejected(t, "completion after cancellation", accepted, err)

	accepted, err = canceler.CancelRun(t.Context(), claim.RunID)
	requireRejected(t, "repeat cancellation", accepted, err)

	accepted, err = canceler.CancelRun(t.Context(), "missing-run")
	requireRejected(t, "missing run cancellation", accepted, err)
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
	requireHeartbeat(t, accepted, expiry, first.Lease.ExpiresAt, err)

	if expireErr := harness.ExpireLease(t.Context(), database, first.RunID, first.NodeID); expireErr != nil {
		t.Fatal(expireErr)
	}

	recovered, err := store.RecoverExpiredLeases(t.Context())
	requireCount(t, "recover lease", recovered, 1, err)

	second := mustClaim(t, store, workerB)
	requireRenewedClaim(t, second, first)

	accepted, err = store.CompleteNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"stale"`))
	requireRejected(t, "expired lease completion", accepted, err)
}

func runRestartAndResume(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "restart")
	database, store := opened.database, opened.backend

	plan := singleNodePlan("conformance-restart", "restart")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, "departed-worker")
	if expireErr := harness.ExpireLease(t.Context(), database, first.RunID, first.NodeID); expireErr != nil {
		t.Fatal(expireErr)
	}

	restarted, err := harness.NewBackend(database)
	if err != nil {
		t.Fatal(err)
	}

	recovered, recoverErr := restarted.RecoverExpiredLeases(t.Context())
	requireCount(t, "restart recovery", recovered, 1, recoverErr)

	second := mustClaim(t, restarted, "resumed-worker")

	accepted, err := restarted.CompleteNode(t.Context(), second.RunID, second.NodeID, second.Lease, []byte(`"resumed"`))
	requireAccepted(t, "resume completion", accepted, err)
}

func runMigrationIdempotence(t *testing.T, harness Harness) {
	t.Helper()

	database := harness.Open(t, "migration-idempotence")
	if err := harness.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}

	if err := harness.Migrate(t.Context(), database); err != nil {
		t.Fatalf("second migration: %v", err)
	}

	if _, err := harness.NewBackend(database); err != nil {
		t.Fatal(err)
	}
}

func runRunDeletion(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "run-deletion")
	database, backend := opened.database, opened.backend

	plan := joinPlan("conformance-run-deletion")
	if err := backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	if err := harness.DeleteRun(t.Context(), database, plan.Run.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := backend.GetRunResult(t.Context(), plan.Run.ID); !errors.Is(err, storage.ErrRunNotFound) {
		t.Fatalf("deleted run error = %v, want %v", err, storage.ErrRunNotFound)
	}
}

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
		t.Context(), owner, time.Minute, []storage.FunctionRegistration{
			{Key: "example.com/Step", Signature: "signature"},
			{Key: "example.com/Left", Signature: "left"},
			{Key: "example.com/Right", Signature: "right"},
			{Key: "example.com/Join", Signature: "join"},
		})
	if err != nil || !claimed || claim == nil {
		t.Fatalf("claim node: claim=%#v claimed=%v err=%v", claim, claimed, err)
	}

	return claim
}

func claimAny(ctx context.Context, backend storage.Backend, owner string) (*storage.Claim, bool, error) {
	claim, claimed, err := backend.ClaimReadyNodeForFunctions(ctx, owner, time.Minute, []storage.FunctionRegistration{
		{Key: "example.com/Step", Signature: "signature"},
		{Key: "example.com/Left", Signature: "left"},
		{Key: "example.com/Right", Signature: "right"},
		{Key: "example.com/Join", Signature: "join"},
	})
	if err != nil {
		return claim, claimed, fmt.Errorf("claim ready node: %w", err)
	}

	return claim, claimed, nil
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

func requireRunResult(t *testing.T, result storage.RunResult, status storage.RunStatus, output, runError []byte) {
	t.Helper()

	if result.Status != status || string(result.Output) != string(output) || string(result.Error) != string(runError) {
		t.Fatalf("run result = %#v, want status=%s output=%q error=%q", result, status, output, runError)
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

func requireHeartbeat(t *testing.T, accepted bool, expiry, previous time.Time, err error) {
	t.Helper()

	if err != nil || !accepted || !expiry.After(previous) {
		t.Fatalf("heartbeat: accepted=%v expiry=%v previous=%v err=%v", accepted, expiry, previous, err)
	}
}

func requireCount(t *testing.T, operation string, got, want int64, err error) {
	t.Helper()

	if err != nil || got != want {
		t.Fatalf("%s: count=%d want=%d err=%v", operation, got, want, err)
	}
}

func requireRenewedClaim(t *testing.T, current, previous *storage.Claim) {
	t.Helper()

	if current.Attempt != previous.Attempt+1 || current.Lease.Generation <= previous.Lease.Generation {
		t.Fatalf("renewed claim = %#v, previous=%#v", current, previous)
	}
}

func singleNodePlan(runID storage.RunID, name string) storage.RunPlan {
	const maxAttempts = 3

	now := time.Now().UTC().Add(-time.Second)

	return storage.RunPlan{
		Edges: nil,
		Run: storage.Run{
			CompletedAt: nil, Output: nil, Error: nil,
			ID: runID, WorkflowName: name, DefinitionHash: "definition", TerminalNodeID: conformanceNodeID,
			Status: storage.RunRunning, Input: []byte(`"input"`), CreatedAt: now, UpdatedAt: now,
			MaxAttempts: maxAttempts, RetryBaseDelay: time.Millisecond,
			RetryMaxDelay: time.Second, RetryPolicyVersion: 1,
		},
		Nodes: []storage.Node{
			conformanceNode(runID, conformanceNodeID, "example.com/Step", "signature", storage.NodeReady, now, 0),
		},
	}
}

func joinPlan(runID storage.RunID) storage.RunPlan {
	plan := singleNodePlan(runID, "join")
	plan.Run.TerminalNodeID = joinNodeID
	plan.Nodes = []storage.Node{
		conformanceNode(runID, leftNodeID, "example.com/Left", "left", storage.NodeReady, plan.Run.CreatedAt, 0),
		conformanceNode(runID, rightNodeID, "example.com/Right", "right", storage.NodeReady, plan.Run.CreatedAt, 0),
		conformanceNode(
			runID, joinNodeID, "example.com/Join", "join",
			storage.NodePending, plan.Run.CreatedAt, joinDependencies,
		),
	}
	plan.Edges = []storage.Edge{
		{RunID: runID, Parent: rightNodeID, Child: joinNodeID, ParentOrder: 0},
		{RunID: runID, Parent: leftNodeID, Child: joinNodeID, ParentOrder: 1},
	}

	return plan
}

func conformanceNode(
	runID storage.RunID,
	identifier storage.NodeID,
	functionKey, signature string,
	status storage.NodeStatus,
	availableAt time.Time,
	remainingDependencies int,
) storage.Node {
	return storage.Node{
		CompletedAt: nil, StartedAt: nil, Lease: storage.Lease{}, Error: nil, Output: nil,
		RunID: runID, ID: identifier, FunctionKey: functionKey, SignatureHash: signature,
		Status: status, AvailableAt: availableAt, RemainingDeps: remainingDependencies, Attempt: 0,
	}
}
