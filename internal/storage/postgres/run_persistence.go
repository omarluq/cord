package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// CreateRun atomically persists a complete run plan. It deliberately retains
// duplicate-insert behavior instead of attaching by idempotency identity.
func (s *Store) CreateRun(ctx context.Context, plan *storage.RunPlan) error {
	validationErr := storage.ValidateRunPlan(plan)
	if validationErr != nil {
		return fmt.Errorf("create run: %w", validationErr)
	}

	return runTransaction(ctx, s.pool, "create run", func(transaction *sql.Tx) error {
		createdAt, err := databaseInstant(ctx, transaction)
		if err != nil {
			return err
		}

		inserted, err := insertRun(ctx, transaction, &plan.Run, false, createdAt)
		if err != nil {
			return err
		}

		if !inserted {
			return fmt.Errorf("insert run %q: no row returned", plan.Run.ID)
		}

		if nodesErr := insertNodes(ctx, transaction, plan, createdAt); nodesErr != nil {
			return nodesErr
		}

		return insertEdges(ctx, transaction, plan)
	})
}

// CreateOrAttachRun atomically persists a complete run plan or attaches to a
// compatible retained run selected by the plan's idempotency key.
func (s *Store) CreateOrAttachRun(
	ctx context.Context,
	plan *storage.RunPlan,
) (runID storage.RunID, created bool, err error) {
	validationErr := storage.ValidateRunPlan(plan)
	if validationErr != nil {
		return "", false, fmt.Errorf("create run: %w", validationErr)
	}

	err = runTransaction(ctx, s.pool, "create run", func(transaction *sql.Tx) error {
		runID, created = "", false

		createdAt, timeErr := databaseInstant(ctx, transaction)
		if timeErr != nil {
			return timeErr
		}

		inserted, insertErr := insertRun(ctx, transaction, &plan.Run, true, createdAt)
		if insertErr != nil {
			return insertErr
		}

		if !inserted {
			attachedID, attachErr := attachRun(ctx, transaction, &plan.Run)
			if attachErr != nil {
				return attachErr
			}

			runID = attachedID

			return nil
		}

		if nodesErr := insertNodes(ctx, transaction, plan, createdAt); nodesErr != nil {
			return nodesErr
		}

		if edgesErr := insertEdges(ctx, transaction, plan); edgesErr != nil {
			return edgesErr
		}

		runID, created = plan.Run.ID, true

		return nil
	})
	if err != nil {
		return "", false, err
	}

	return runID, created, nil
}

func insertRun(
	ctx context.Context,
	transaction *sql.Tx,
	run *storage.Run,
	attach bool,
	createdAt time.Time,
) (bool, error) {
	query := `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, output_payload,
		terminal_node_id, error_payload, created_at, updated_at, completed_at,
		max_attempts, retry_base_delay_ns, retry_max_delay_ns, retry_policy_version,
		idempotency_key, submission_fingerprint
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$10,$11,$12,$13,$14,$15,$16)`
	if attach && run.IdempotencyKey != nil {
		query += ` ON CONFLICT (workflow_name, idempotency_key) DO NOTHING`
	}

	query += ` RETURNING id`

	var insertedID storage.RunID

	err := transaction.QueryRowContext(
		ctx,
		query,
		run.ID,
		run.WorkflowName,
		run.DefinitionHash,
		run.Status,
		[]byte(run.Input),
		nullablePayload(run.Output),
		run.TerminalNodeID,
		nullablePayload(run.Error),
		createdAt,
		nullableTimePointer(run.CompletedAt),
		run.MaxAttempts,
		run.RetryBaseDelay.Nanoseconds(),
		run.RetryMaxDelay.Nanoseconds(),
		run.RetryPolicyVersion,
		nullableStringPointer(run.IdempotencyKey),
		nullableStringPointer(run.SubmissionFingerprint),
	).Scan(&insertedID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("insert run %q: %w", run.ID, err)
	}

	return true, nil
}

func attachRun(
	ctx context.Context,
	transaction *sql.Tx,
	run *storage.Run,
) (storage.RunID, error) {
	const query = `SELECT id, definition_hash, submission_fingerprint
		FROM cord_runs
		WHERE workflow_name = $1 AND idempotency_key = $2
		FOR UPDATE`

	var (
		retainedID            storage.RunID
		definitionHash        string
		submissionFingerprint sql.NullString
	)
	if err := transaction.QueryRowContext(
		ctx, query, run.WorkflowName, nullableStringPointer(run.IdempotencyKey),
	).Scan(&retainedID, &definitionHash, &submissionFingerprint); err != nil {
		return "", fmt.Errorf("read retained idempotent run: %w", err)
	}

	if definitionHash != run.DefinitionHash ||
		!submissionFingerprint.Valid ||
		submissionFingerprint.String != *run.SubmissionFingerprint {
		return "", fmt.Errorf(
			"attach run for workflow %q and idempotency key: %w",
			run.WorkflowName,
			storage.ErrRunConflict,
		)
	}

	return retainedID, nil
}
