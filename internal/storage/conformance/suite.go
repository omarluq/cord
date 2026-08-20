// Package conformance verifies storage backend behavior.
package conformance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
)

// Driver configures a database/sql driver for the conformance suite.
type Driver struct {
	// DataSource builds a driver data source from a temporary path and busy timeout.
	DataSource func(string, time.Duration) string
	// Open optionally opens the database and takes precedence over Name and DataSource.
	Open func(testing.TB) *sql.DB
	// Name identifies the registered database/sql driver.
	Name string
	// SkipWriteContention skips the local-file write-contention test.
	SkipWriteContention bool
}

// RepeatedPragmaDataSource builds a data source for drivers that accept repeated _pragma parameters.
func RepeatedPragmaDataSource(path string, timeout time.Duration) string {
	query := url.Values{}
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", timeout.Milliseconds()))
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "journal_mode(WAL)")

	return "file:" + path + "?" + query.Encode()
}

// UnderscoreDataSource builds a data source for drivers that accept underscore-prefixed options.
func UnderscoreDataSource(path string, timeout time.Duration) string {
	query := url.Values{}
	query.Set("_busy_timeout", strconv.FormatInt(timeout.Milliseconds(), 10))
	query.Set("_foreign_keys", "on")
	query.Set("_journal_mode", "WAL")

	return "file:" + path + "?" + query.Encode()
}

const (
	conformanceNodeID  storage.NodeID = "node"
	leftNodeID         storage.NodeID = "left"
	rightNodeID        storage.NodeID = "right"
	joinNodeID         storage.NodeID = "join"
	storeBusyTimeout                  = 5 * time.Second
	heartbeatExtension                = 2 * time.Minute
	joinDependencies                  = 2
)

// Run executes Cord's behavioral storage conformance suite.
func Run(t *testing.T, driver Driver) {
	t.Helper()

	open := func(tb testing.TB, path string, timeout time.Duration) *sql.DB {
		tb.Helper()

		var database *sql.DB
		if driver.Open != nil {
			database = driver.Open(tb)
		} else {
			var err error

			database, err = sql.Open(driver.Name, driver.DataSource(path, timeout))
			if err != nil {
				tb.Fatal(err)
			}
		}

		tb.Cleanup(func() {
			if err := database.Close(); err != nil {
				tb.Errorf("close database: %v", err)
			}
		})

		return database
	}

	tests := []struct {
		run  func(*testing.T, Open)
		name string
	}{
		{name: "workflow", run: runWorkflow},
		{name: "create and result", run: runCreateAndResult},
		{name: "join order and dependency release", run: runJoinOrder},
		{name: "claim uniqueness and completion fence", run: runClaimAndCompletionFence},
		{name: "retry and promotion", run: runRetryAndPromotion},
		{name: "failure", run: runFailure},
		{name: "heartbeat and recovery", run: runHeartbeatAndRecovery},
		{name: "cancellation", run: runCancellation},
		{name: "restart and resume", run: runRestartAndResume},
		{name: "migration idempotence and foreign keys", run: runMigrationAndForeignKeys},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.run(t, open)
		})
	}

	if !driver.SkipWriteContention {
		t.Run("write contention", func(t *testing.T) { runContention(t, open) })
	}
}

// Open opens a backend-specific test database.
type Open func(testing.TB, string, time.Duration) *sql.DB

func runWorkflow(t *testing.T, open Open) {
	t.Helper()

	const (
		busyTimeout = 5 * time.Second
		input       = 2
		want        = 4
	)

	database := open(t, filepath.Join(t.TempDir(), "workflow.db"), busyTimeout)

	runtime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		closeErr := runtime.Close()
		if closeErr != nil {
			t.Errorf("close runtime: %v", closeErr)
		}
	})

	workflow := runtime.From("sqlite-driver-conformance", timesTwo)

	result, err := workflow.Run(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}

	if result != want {
		t.Fatalf("result = %d, want %d", result, want)
	}
}

func runCreateAndResult(t *testing.T, open Open) {
	t.Helper()

	_, store := openStore(t, open, "create-result")
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

func runJoinOrder(t *testing.T, open Open) {
	t.Helper()

	_, store := openStore(t, open, "join")

	plan := joinPlan("conformance-join")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, "worker-a")

	second := mustClaim(t, store, "worker-b")
	requireNodeIDs(t, first, second, leftNodeID, rightNodeID)

	accepted, err := store.CompleteNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"left"`))
	requireAccepted(t, "complete left", accepted, err)

	earlyClaim, earlyClaimed, earlyErr := store.ClaimReadyNode(t.Context(), "worker-early", time.Minute)
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

func runClaimAndCompletionFence(t *testing.T, open Open) {
	t.Helper()

	_, store := openStore(t, open, "claim-fence")

	plan := singleNodePlan("conformance-claim", "claim")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, store, "winner")
	if duplicate, claimed, err := store.ClaimReadyNode(t.Context(), "loser", time.Minute); err != nil || claimed {
		t.Fatalf("duplicate claim = %#v, claimed=%v err=%v", duplicate, claimed, err)
	}

	stale := claim.Lease
	stale.Generation--

	accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, stale, []byte(`"stale"`))
	if err != nil || accepted {
		t.Fatalf("stale completion: accepted=%v err=%v", accepted, err)
	}

	accepted, err = store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"current"`))
	if err != nil || !accepted {
		t.Fatalf("owned completion: accepted=%v err=%v", accepted, err)
	}
}

func runRetryAndPromotion(t *testing.T, open Open) {
	t.Helper()

	_, store := openStore(t, open, "retry")

	plan := singleNodePlan("conformance-retry", "retry")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, "worker-a")

	accepted, err := store.RetryNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"retry"`), 0)
	if err != nil || !accepted {
		t.Fatalf("retry node: accepted=%v err=%v", accepted, err)
	}

	promoted, err := store.PromoteRetries(t.Context())
	if err != nil || promoted != 1 {
		t.Fatalf("promote retries: count=%d err=%v", promoted, err)
	}

	second := mustClaim(t, store, "worker-b")
	if second.Attempt != 2 || second.Lease.Generation <= first.Lease.Generation {
		t.Fatalf("retry claim = %#v, first generation=%d", second, first.Lease.Generation)
	}
}

func runFailure(t *testing.T, open Open) {
	t.Helper()

	_, store := openStore(t, open, "failure")

	plan := joinPlan("conformance-failure")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, store, "worker")
	failure := []byte(`{"message":"permanent"}`)

	accepted, err := store.FailNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, failure)
	if err != nil || !accepted {
		t.Fatalf("fail node: accepted=%v err=%v", accepted, err)
	}

	result, err := store.GetRunResult(t.Context(), claim.RunID)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != storage.RunFailed || string(result.Error) != string(failure) {
		t.Fatalf("failed result = %#v", result)
	}

	if next, claimed, claimErr := store.ClaimReadyNode(t.Context(), "other", time.Minute); claimErr != nil || claimed {
		t.Fatalf("claim after failure = %#v, claimed=%v err=%v", next, claimed, claimErr)
	}
}

func runHeartbeatAndRecovery(t *testing.T, open Open) {
	t.Helper()

	database, store := openStore(t, open, "lease")

	plan := singleNodePlan("conformance-lease", "lease")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, "worker-a")

	accepted, expiry, err := store.HeartbeatNode(
		t.Context(), first.RunID, first.NodeID, first.Lease, heartbeatExtension,
	)
	requireHeartbeat(t, accepted, expiry, first.Lease.ExpiresAt, err)

	_, expireErr := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET lease_expires_at = '2000-01-01T00:00:00.000Z' WHERE run_id = ? AND node_id = ?`,
		first.RunID, first.NodeID)
	if expireErr != nil {
		t.Fatal(expireErr)
	}

	recovered, err := store.RecoverExpiredLeases(t.Context())
	requireCount(t, "recover lease", recovered, 1, err)

	second := mustClaim(t, store, "worker-b")
	requireRenewedClaim(t, second, first)

	accepted, err = store.CompleteNode(t.Context(), first.RunID, first.NodeID, first.Lease, []byte(`"stale"`))
	requireRejected(t, "expired lease completion", accepted, err)
}

func runCancellation(t *testing.T, open Open) {
	t.Helper()

	_, store := openStore(t, open, "cancel")

	plan := joinPlan("conformance-cancel")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	claim := mustClaim(t, store, "worker")

	accepted, err := store.CancelRun(t.Context(), claim.RunID)
	if err != nil || !accepted {
		t.Fatalf("cancel run: accepted=%v err=%v", accepted, err)
	}

	result, err := store.GetRunResult(t.Context(), claim.RunID)
	if err != nil || result.Status != storage.RunCanceled {
		t.Fatalf("canceled result = %#v, err=%v", result, err)
	}

	accepted, err = store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"late"`))
	if err != nil || accepted {
		t.Fatalf("completion after cancellation: accepted=%v err=%v", accepted, err)
	}
}

func runRestartAndResume(t *testing.T, open Open) {
	t.Helper()

	database, store := openStore(t, open, "restart")

	plan := singleNodePlan("conformance-restart", "restart")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, "departed-worker")
	if _, err := database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET lease_expires_at = '2000-01-01T00:00:00.000Z' WHERE run_id = ? AND node_id = ?`,
		first.RunID, first.NodeID); err != nil {
		t.Fatal(err)
	}

	restarted, err := sqlite.New(database)
	if err != nil {
		t.Fatal(err)
	}

	if recovered, recoverErr := restarted.RecoverExpiredLeases(t.Context()); recoverErr != nil || recovered != 1 {
		t.Fatalf("restart recovery: count=%d err=%v", recovered, recoverErr)
	}

	second := mustClaim(t, restarted, "resumed-worker")

	accepted, err := restarted.CompleteNode(t.Context(), second.RunID, second.NodeID, second.Lease, []byte(`"resumed"`))
	if err != nil || !accepted {
		t.Fatalf("resume completion: accepted=%v err=%v", accepted, err)
	}
}

func runMigrationAndForeignKeys(t *testing.T, open Open) {
	t.Helper()

	database := open(t, filepath.Join(t.TempDir(), "migration.db"), storeBusyTimeout)
	if err := sqlite.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}

	if err := sqlite.Migrate(t.Context(), database); err != nil {
		t.Fatalf("second migration: %v", err)
	}

	store, err := sqlite.New(database)
	if err != nil {
		t.Fatal(err)
	}

	plan := joinPlan("conformance-foreign-key")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	if _, err := database.ExecContext(t.Context(), "DELETE FROM cord_runs WHERE id = ?", plan.Run.ID); err != nil {
		t.Fatal(err)
	}

	queries := []struct{ table, query string }{
		{table: "cord_nodes", query: "SELECT COUNT(*) FROM cord_nodes WHERE run_id = ?"},
		{table: "cord_edges", query: "SELECT COUNT(*) FROM cord_edges WHERE run_id = ?"},
	}
	for _, current := range queries {
		var count int

		row := database.QueryRowContext(t.Context(), current.query, plan.Run.ID)
		if err := row.Scan(&count); err != nil {
			t.Fatal(err)
		}

		if count != 0 {
			t.Fatalf("%s rows after run deletion = %d, want 0", current.table, count)
		}
	}
}

func openStore(t *testing.T, open Open, name string) (*sql.DB, *sqlite.Store) {
	t.Helper()

	database := open(t, filepath.Join(t.TempDir(), name+".db"), storeBusyTimeout)
	if err := sqlite.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}

	store, err := sqlite.New(database)
	if err != nil {
		t.Fatal(err)
	}

	return database, store
}

func mustClaim(t *testing.T, store *sqlite.Store, owner string) *storage.Claim {
	t.Helper()

	claim, claimed, err := store.ClaimReadyNode(t.Context(), owner, time.Minute)
	if err != nil || !claimed || claim == nil {
		t.Fatalf("claim node: claim=%#v claimed=%v err=%v", claim, claimed, err)
	}

	return claim
}

func runContention(t *testing.T, open Open) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "contention.db")
	first := open(t, path, 0)
	second := open(t, path, 0)

	if err := sqlite.Migrate(t.Context(), first); err != nil {
		t.Fatal(err)
	}

	store, err := sqlite.New(second)
	if err != nil {
		t.Fatal(err)
	}

	transaction, err := first.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("rollback transaction: %v", err)
		}
	})

	if _, err := transaction.ExecContext(t.Context(), "UPDATE cord_runs SET status = status"); err != nil {
		t.Fatal(err)
	}

	verifyRetryWhileLocked(t, store, transaction)
}

func verifyRetryWhileLocked(t *testing.T, store *sqlite.Store, transaction *sql.Tx) {
	t.Helper()

	const lockObservationDelay = 20 * time.Millisecond

	now := time.Now().UTC()
	runID := storage.RunID(fmt.Sprintf("sqlite-driver-contention-%d", now.UnixNano()))

	result := make(chan error, 1)
	go func() {
		result <- store.CreateRun(t.Context(), contentionPlan(runID, now))
	}()

	select {
	case err := <-result:
		t.Fatalf("create run returned while the write lock was held: %v", err)
	case <-time.After(lockObservationDelay):
	}

	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := <-result; err != nil {
		t.Fatal(err)
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

func timesTwo(_ context.Context, input int) (int, error) {
	const multiplier = 2

	return input * multiplier, nil
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

func contentionPlan(runID storage.RunID, now time.Time) *storage.RunPlan {
	return &storage.RunPlan{
		Edges: nil,
		Run: storage.Run{
			CompletedAt: nil, Output: nil, Error: nil,
			ID: runID, WorkflowName: "sqlite-driver-contention", DefinitionHash: "definition",
			TerminalNodeID: "node", Status: storage.RunRunning, Input: []byte(`1`), CreatedAt: now, UpdatedAt: now,
			MaxAttempts: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond, RetryPolicyVersion: 1,
		},
		Nodes: []storage.Node{{
			CompletedAt: nil, StartedAt: nil, Lease: storage.Lease{}, Error: nil, Output: nil,
			RunID: runID, ID: "node", FunctionKey: "example.com/Step", SignatureHash: "signature",
			Status: storage.NodeReady, AvailableAt: now, RemainingDeps: 0, Attempt: 0,
		}},
	}
}
