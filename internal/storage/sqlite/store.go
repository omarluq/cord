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

// CreateRun atomically persists a complete run plan. It deliberately retains
// duplicate-insert behavior instead of attaching by idempotency identity.
func (s *Store) CreateRun(ctx context.Context, plan *storage.RunPlan) error {
	if validationErr := storage.ValidateRunPlan(plan); validationErr != nil {
		return fmt.Errorf("create run: %w", validationErr)
	}

	return retryContention(ctx, "retry run plan", func(attemptCtx context.Context) error {
		return s.createRunOnlyOnce(attemptCtx, plan)
	})
}

func (s *Store) createRunOnlyOnce(ctx context.Context, plan *storage.RunPlan) error {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin run-plan transaction: %w", err)
	}

	var createdAt time.Time
	if err = requireForeignKeys(ctx, transaction); err == nil {
		createdAt, err = databaseInstant(ctx, transaction)
	}

	if err == nil {
		err = insertRun(ctx, transaction, &plan.Run, createdAt)
	}

	if err == nil {
		err = s.createRunContents(ctx, transaction, plan, createdAt)
	}

	if err != nil {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil {
			return fmt.Errorf("persist run plan: %w", errors.Join(err, rollbackErr))
		}

		return err
	}

	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("commit run plan: %w", err)
	}

	return nil
}

// CreateOrAttachRun atomically persists a complete run plan or attaches to the
// retained compatible run selected by its idempotency key.
func (s *Store) CreateOrAttachRun(
	ctx context.Context,
	plan *storage.RunPlan,
) (runID storage.RunID, created bool, err error) {
	if validationErr := storage.ValidateRunPlan(plan); validationErr != nil {
		return "", false, fmt.Errorf("create run: %w", validationErr)
	}

	err = retryContention(ctx, "retry run plan", func(attemptCtx context.Context) error {
		runID, created, err = s.createRunOnce(attemptCtx, plan)

		return err
	})
	if err != nil {
		return "", false, err
	}

	return runID, created, nil
}

func (s *Store) createRunOnce(
	ctx context.Context,
	plan *storage.RunPlan,
) (runID storage.RunID, created bool, err error) {
	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return "", false, fmt.Errorf("begin run-plan transaction: %w", err)
	}

	defer func() { err = rollbackError(transaction, "rollback run-plan transaction", err) }()

	validationErr := requireForeignKeys(ctx, transaction)
	if validationErr != nil {
		return "", false, validationErr
	}

	createdAt, err := databaseInstant(ctx, transaction)
	if err != nil {
		return "", false, err
	}

	created = true

	if insertErr := insertRun(ctx, transaction, &plan.Run, createdAt); insertErr != nil {
		runID, insertErr = attachCompatibleRun(ctx, transaction, plan, insertErr)
		created = false

		if insertErr != nil {
			return "", false, insertErr
		}
	} else {
		runID = plan.Run.ID
		if createErr := s.createRunContents(ctx, transaction, plan, createdAt); createErr != nil {
			return "", false, createErr
		}
	}

	if err := transaction.Commit(); err != nil {
		return "", false, fmt.Errorf("commit run plan: %w", err)
	}

	return runID, created, nil
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

func (s *Store) createRunContents(
	ctx context.Context,
	transaction *sql.Tx,
	plan *storage.RunPlan,
	createdAt time.Time,
) error {
	for index := range plan.Nodes {
		if err := insertNode(ctx, transaction, plan.Run.ID, &plan.Nodes[index], createdAt); err != nil {
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

func attachCompatibleRun(
	ctx context.Context,
	transaction *sql.Tx,
	plan *storage.RunPlan,
	insertErr error,
) (storage.RunID, error) {
	if plan.Run.IdempotencyKey == nil {
		return "", insertErr
	}

	var (
		existingID            storage.RunID
		definitionHash        string
		submissionFingerprint sql.NullString
	)

	err := transaction.QueryRowContext(ctx, `SELECT id, definition_hash, submission_fingerprint
		FROM cord_runs WHERE workflow_name = ? AND idempotency_key = ?`,
		plan.Run.WorkflowName, *plan.Run.IdempotencyKey,
	).Scan(&existingID, &definitionHash, &submissionFingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return "", insertErr
	}

	if err != nil {
		return "", fmt.Errorf("inspect idempotent run attachment: %w", err)
	}

	if definitionHash != plan.Run.DefinitionHash || !submissionFingerprint.Valid ||
		plan.Run.SubmissionFingerprint == nil || submissionFingerprint.String != *plan.Run.SubmissionFingerprint {
		return "", fmt.Errorf(
			"attach run for workflow %q and idempotency key: %w",
			plan.Run.WorkflowName,
			storage.ErrRunConflict,
		)
	}

	return existingID, nil
}

func insertRun(
	ctx context.Context,
	transaction *sql.Tx,
	run *storage.Run,
	createdAt time.Time,
) error {
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
		formatTime(createdAt),
		formatTime(createdAt),
		nullTimePointer(run.CompletedAt),
		run.MaxAttempts,
		run.RetryBaseDelay.Nanoseconds(),
		run.RetryMaxDelay.Nanoseconds(),
		run.RetryPolicyVersion,
		nullStringPointer(run.IdempotencyKey),
		nullStringPointer(run.SubmissionFingerprint),
		storage.LifecycleVersion1,
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
	createdAt time.Time,
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
		storage.LifecycleVersion1,
		formatTime(createdAt),
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

func nullStringPointer(value *string) any {
	if value == nil {
		return nil
	}

	return *value
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

func databaseInstant(ctx context.Context, transaction *sql.Tx) (time.Time, error) {
	var value string
	if err := transaction.QueryRowContext(
		ctx,
		`SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
	).Scan(&value); err != nil {
		return time.Time{}, fmt.Errorf("read database time: %w", err)
	}

	instant, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse database time: %w", err)
	}

	return instant.UTC(), nil
}
