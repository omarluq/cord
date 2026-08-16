package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// Store persists workflow state in a caller-owned SQL database.
type Store struct {
	database *sql.DB
}

var _ storage.Backend = (*Store)(nil)

// New creates a SQLite store for a caller-owned SQL database. Callers should
// configure a busy timeout and use WAL journal mode where appropriate.
func New(database *sql.DB) (*Store, error) {
	if database == nil {
		return nil, errors.New("create storage store: database is nil")
	}

	return &Store{database: database}, nil
}

// CreateRun atomically persists a complete run plan.
func (s *Store) CreateRun(ctx context.Context, plan *storage.RunPlan) error {
	if err := storage.ValidateRunPlan(plan); err != nil {
		return fmt.Errorf("create run: %w", err)
	}

	return retryContention(ctx, "retry run plan", func() error {
		return s.createRunOnce(ctx, plan)
	})
}

func (s *Store) createRunOnce(ctx context.Context, plan *storage.RunPlan) error {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin run-plan transaction: %w", err)
	}

	if err := requireForeignKeys(ctx, transaction); err != nil {
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

func requireForeignKeys(ctx context.Context, transaction *sql.Tx) error {
	var enabled bool
	if err := transaction.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&enabled); err != nil {
		return fmt.Errorf("inspect sqlite foreign-key enforcement: %w", err)
	}

	if !enabled {
		return errors.New("sqlite foreign-key enforcement is disabled; enable it for every connection")
	}

	return nil
}

func (s *Store) createRun(ctx context.Context, transaction *sql.Tx, plan *storage.RunPlan) error {
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

func insertRun(ctx context.Context, transaction *sql.Tx, run *storage.Run) error {
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
		formatTime(run.CreatedAt),
		formatTime(run.UpdatedAt),
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
	runID storage.RunID,
	node *storage.Node,
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
		formatTime(node.AvailableAt),
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
	runID storage.RunID,
	edge storage.Edge,
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

func nullPayload(payload storage.EncodedPayload) any {
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

	return formatTime(value)
}

func nullTimePointer(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}

	return formatTime(*value)
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
