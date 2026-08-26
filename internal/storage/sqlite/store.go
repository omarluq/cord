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
		return joinRollbackError(transaction.Rollback(), "persist run plan", err)
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

	defer func() {
		err = joinRollbackError(transaction.Rollback(), "rollback run-plan transaction", err)
	}()

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
