package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RunPlan is the complete normalized storage plan for one run.
type RunPlan struct {
	Nodes []Node
	Edges []Edge
	Run   Run
}

// Store persists workflow state in a caller-owned SQL database.
type Store struct {
	database *sql.DB
}

// NewStore creates a SQLite store for a caller-owned SQL database. Callers
// should configure a busy timeout and use WAL journal mode where appropriate.
func NewStore(database *sql.DB) (*Store, error) {
	if database == nil {
		return nil, errors.New("create storage store: database is nil")
	}

	return &Store{database: database}, nil
}

// CreateRun atomically persists a complete run plan.
func (s *Store) CreateRun(ctx context.Context, plan *RunPlan) error {
	if err := validateRunPlan(plan); err != nil {
		return err
	}

	return retrySQLiteContention(ctx, "retry run plan", func() error {
		return s.createRunOnce(ctx, plan)
	})
}

func (s *Store) createRunOnce(ctx context.Context, plan *RunPlan) error {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin run-plan transaction: %w", err)
	}

	if err := requireSQLiteForeignKeys(ctx, transaction); err != nil {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil {
			return fmt.Errorf("validate run-plan transaction: %w", errors.Join(err, rollbackErr))
		}

		return err
	}

	if err := s.createRun(ctx, transaction, plan); err != nil {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil {
			return fmt.Errorf("persist run plan: %w", errors.Join(err, rollbackErr))
		}

		return err
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit run plan: %w", err)
	}

	return nil
}

func requireSQLiteForeignKeys(ctx context.Context, transaction *sql.Tx) error {
	var enabled bool
	if err := transaction.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&enabled); err != nil {
		return fmt.Errorf("inspect sqlite foreign-key enforcement: %w", err)
	}

	if !enabled {
		return errors.New("sqlite foreign-key enforcement is disabled; enable it for every connection")
	}

	return nil
}

func (s *Store) createRun(ctx context.Context, transaction *sql.Tx, plan *RunPlan) error {
	if err := insertRun(ctx, transaction, &plan.Run); err != nil {
		return err
	}

	for index := range plan.Nodes {
		if err := insertNode(ctx, transaction, plan.Run.ID, &plan.Nodes[index]); err != nil {
			return err
		}
	}

	for _, edge := range plan.Edges {
		if err := insertEdge(ctx, transaction, plan.Run.ID, edge); err != nil {
			return err
		}
	}

	return nil
}

func validateRunPlan(plan *RunPlan) error {
	if plan == nil {
		return errors.New("validate run plan: plan is nil")
	}

	if plan.Run.ID == "" {
		return errors.New("validate run plan: run ID is empty")
	}

	if err := validateRetryPolicy(&plan.Run); err != nil {
		return err
	}

	dependencies := make(map[NodeID]int, len(plan.Nodes))
	for index := range plan.Nodes {
		current := &plan.Nodes[index]
		if current.RunID != plan.Run.ID {
			return fmt.Errorf("validate run plan: node %q has run ID %q", current.ID, current.RunID)
		}

		if _, exists := dependencies[current.ID]; exists {
			return fmt.Errorf("validate run plan: duplicate node %q", current.ID)
		}

		dependencies[current.ID] = 0
	}

	if _, exists := dependencies[plan.Run.TerminalNodeID]; !exists {
		return fmt.Errorf("validate run plan: terminal node %q does not exist", plan.Run.TerminalNodeID)
	}

	return validateEdges(plan, dependencies)
}

func validateRetryPolicy(run *Run) error {
	if run.MaxAttempts < 1 || run.RetryBaseDelay <= 0 ||
		run.RetryMaxDelay < run.RetryBaseDelay || run.RetryPolicyVersion < 1 {
		return errors.New("validate run plan: retry policy is invalid")
	}

	return nil
}

func validateEdges(plan *RunPlan, dependencies map[NodeID]int) error {
	children := make(map[NodeID][]NodeID, len(plan.Nodes))

	for _, edge := range plan.Edges {
		if edge.RunID != plan.Run.ID {
			return fmt.Errorf("validate run plan: edge %q -> %q has run ID %q", edge.Parent, edge.Child, edge.RunID)
		}

		if _, exists := dependencies[edge.Parent]; !exists {
			return fmt.Errorf("validate run plan: edge parent %q does not exist", edge.Parent)
		}

		if _, exists := dependencies[edge.Child]; !exists {
			return fmt.Errorf("validate run plan: edge child %q does not exist", edge.Child)
		}

		dependencies[edge.Child]++
		children[edge.Parent] = append(children[edge.Parent], edge.Child)
	}

	if cyclic(dependencies, children) {
		return errors.New("validate run plan: edges contain a cycle")
	}

	for index := range plan.Nodes {
		current := &plan.Nodes[index]
		if current.RemainingDeps != dependencies[current.ID] {
			return fmt.Errorf("validate run plan: node %q dependency count does not match edges", current.ID)
		}

		expected := NodePending
		if current.RemainingDeps == 0 {
			expected = NodeReady
		}

		if current.Status != expected {
			return fmt.Errorf("validate run plan: node %q must initially be %q", current.ID, expected)
		}
	}

	return nil
}

func cyclic(dependencies map[NodeID]int, children map[NodeID][]NodeID) bool {
	remaining := make(map[NodeID]int, len(dependencies))
	ready := make([]NodeID, 0, len(dependencies))

	for nodeID, dependencyCount := range dependencies {
		remaining[nodeID] = dependencyCount
		if dependencyCount == 0 {
			ready = append(ready, nodeID)
		}
	}

	visited := 0

	for len(ready) > 0 {
		current := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		visited++

		for _, child := range children[current] {
			remaining[child]--
			if remaining[child] == 0 {
				ready = append(ready, child)
			}
		}
	}

	return visited != len(dependencies)
}

func insertRun(ctx context.Context, transaction *sql.Tx, run *Run) error {
	_, err := transaction.ExecContext(
		ctx,
		insertRunStatement,
		run.ID,
		run.WorkflowName,
		run.DefinitionHash,
		run.Status,
		[]byte(run.Input),
		nullPayload(run.Output),
		run.TerminalNodeID,
		nullPayload(run.Error),
		formatSQLiteTime(run.CreatedAt),
		formatSQLiteTime(run.UpdatedAt),
		nullTimePointer(run.CompletedAt),
		run.MaxAttempts,
		run.RetryBaseDelay.Nanoseconds(),
		run.RetryMaxDelay.Nanoseconds(),
		run.RetryPolicyVersion,
	)
	if err != nil {
		return fmt.Errorf("insert run %q: %w", run.ID, err)
	}

	return nil
}

func insertNode(
	ctx context.Context,
	transaction *sql.Tx,
	runID RunID,
	node *Node,
) error {
	_, err := transaction.ExecContext(
		ctx,
		insertNodeStatement,
		runID,
		node.ID,
		node.FunctionKey,
		node.SignatureHash,
		node.Status,
		node.RemainingDeps,
		node.Attempt,
		formatSQLiteTime(node.AvailableAt),
		nullString(node.Lease.Owner),
		node.Lease.Generation,
		nullTime(node.Lease.ExpiresAt),
		nullPayload(node.Output),
		nullPayload(node.Error),
		nullTimePointer(node.StartedAt),
		nullTimePointer(node.CompletedAt),
	)
	if err != nil {
		return fmt.Errorf("insert node %q for run %q: %w", node.ID, runID, err)
	}

	return nil
}

func insertEdge(
	ctx context.Context,
	transaction *sql.Tx,
	runID RunID,
	edge Edge,
) error {
	_, err := transaction.ExecContext(
		ctx,
		insertEdgeStatement,
		runID,
		edge.Parent,
		edge.Child,
		edge.ParentOrder,
	)
	if err != nil {
		return fmt.Errorf(
			"insert edge %q -> %q for run %q: %w",
			edge.Parent,
			edge.Child,
			runID,
			err,
		)
	}

	return nil
}

func nullPayload(payload EncodedPayload) any {
	if payload == nil {
		return nil
	}

	return []byte(payload)
}

func nullString(value string) any {
	if value == "" {
		return nil
	}

	return value
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}

	return formatSQLiteTime(value)
}

func nullTimePointer(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}

	return formatSQLiteTime(*value)
}

func formatSQLiteTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
