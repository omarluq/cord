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
	// CountRunRows counts node and edge rows belonging to a run.
	CountRunRows func(context.Context, *sql.DB, storage.RunID) (nodes int, edges int, err error)
	// LoadNodeStates loads persisted node state for terminal-transition assertions.
	LoadNodeStates func(context.Context, *sql.DB, storage.RunID) (map[storage.NodeID]NodeState, error)
}

// NodeState is the persisted node state observed by conformance tests.
type NodeState struct {
	Status          storage.NodeStatus
	LeaseOwner      string
	Error           storage.EncodedPayload
	LeaseGeneration int64
	Attempt         int
	HasLeaseExpiry  bool
}

// NewNodeStateLoader returns a harness function that loads persisted node state
// using a backend-specific query with the run ID as its sole argument.
func NewNodeStateLoader(
	backend string,
	query string,
) func(context.Context, *sql.DB, storage.RunID) (map[storage.NodeID]NodeState, error) {
	return func(
		ctx context.Context,
		database *sql.DB,
		runID storage.RunID,
	) (_ map[storage.NodeID]NodeState, err error) {
		rows, err := database.QueryContext(ctx, query, runID)
		if err != nil {
			return nil, fmt.Errorf("load %s node states: %w", backend, err)
		}
		defer func() { err = errors.Join(err, rows.Close()) }()

		states := make(map[storage.NodeID]NodeState)

		for rows.Next() {
			var (
				identifier storage.NodeID
				state      NodeState
				failure    []byte
			)
			if scanErr := rows.Scan(
				&identifier,
				&state.Status,
				&failure,
				&state.LeaseOwner,
				&state.LeaseGeneration,
				&state.HasLeaseExpiry,
				&state.Attempt,
			); scanErr != nil {
				return nil, fmt.Errorf("scan %s node state: %w", backend, scanErr)
			}

			state.Error = storage.EncodedPayload(failure)
			states[identifier] = state
		}

		if rowsErr := rows.Err(); rowsErr != nil {
			return nil, fmt.Errorf("iterate %s node states: %w", backend, rowsErr)
		}

		return states, nil
	}
}

const (
	conformanceNodeID  storage.NodeID = "node"
	leftNodeID         storage.NodeID = "left"
	rightNodeID        storage.NodeID = "right"
	joinNodeID         storage.NodeID = "join"
	stepFunctionKey                   = "example.com/Step"
	leftFunctionKey                   = "example.com/Left"
	rightFunctionKey                  = "example.com/Right"
	joinFunctionKey                   = "example.com/Join"
	heartbeatExtension                = 2 * time.Minute
	joinDependencies                  = 2
	workerA                           = "worker-a"
	workerB                           = "worker-b"
	completedNodeName                 = "completed"
	runningNodeName                   = "running"
	readyNodeName                     = "ready"
	retryingNodeName                  = "retrying"
	pendingNodeName                   = "pending"
	terminalNodeName                  = "terminal"
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
		{name: "inspection snapshots", run: runInspectionSnapshots},
		{name: "inspection is read-only", run: runInspectionIsReadOnly},
		{name: "node pagination", run: runNodePagination},
		{name: "node pagination validation", run: runNodePaginationValidation},
		{name: "idempotent create or attach", run: runIdempotentCreateOrAttach},
		{name: "idempotent create conflict", run: runIdempotentCreateConflict},
		{name: "concurrent idempotent create", run: runConcurrentIdempotentCreate},
		{name: "idempotency deletion release", run: runIdempotencyDeletionRelease},
		{name: "invalid run plans", run: runInvalidRunPlans},
		{name: "join order and dependency release", run: runJoinOrder},
		{name: "claim uniqueness and completion fence", run: runClaimAndCompletionFence},
		{name: "concurrent claim winner metadata", run: runConcurrentClaimWinnerMetadata},
		{name: "completion cancellation race", run: runCompletionCancellationRace},
		{name: "failure cancellation race", run: runFailureCancellationRace},
		{name: "terminal lifecycle absorption", run: runTerminalLifecycleAbsorption},
		{name: "retry and promotion", run: runRetryAndPromotion},
		{name: "failure", run: runFailure},
		{name: "heartbeat and recovery", run: runHeartbeatAndRecovery},
		{name: "stale heartbeat metadata fence", run: runStaleHeartbeatMetadataFence},
		{name: "final attempt lease expiry", run: runFinalAttemptLeaseExpiry},
		{name: "concurrent final attempt recovery", run: runConcurrentFinalAttemptRecovery},
		{name: "restart and resume", run: runRestartAndResume},
		{name: "cancellation outcomes", run: runCancellationOutcomes},
		{name: "cancellation states and fences", run: runCancellationStatesAndFences},
		{name: "deterministic cancellation orderings", run: runDeterministicCancellationOrderings},
		{name: "concurrent cancellation", run: runConcurrentCancellation},
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
		harness.ExpireLease == nil || harness.DeleteRun == nil ||
		harness.CountRunRows == nil || harness.LoadNodeStates == nil {
		t.Fatal("conformance harness requires all lifecycle and operation callbacks")
	}
}
