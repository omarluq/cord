// Package postgres implements Cord's PostgreSQL persistence adapter.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// Store persists workflow state in a caller-owned PostgreSQL database.
type Store struct{ pool *sql.DB }

var _ storage.Backend = (*Store)(nil)

// New creates a PostgreSQL store for a caller-owned SQL database.
func New(database *sql.DB) (*Store, error) {
	if database != nil {
		return &Store{pool: database}, nil
	}

	return nil, errors.New("create postgres store: database is nil")
}

// CreateRun atomically persists a complete run plan. It deliberately retains
// duplicate-insert behavior instead of attaching by idempotency identity.
func (s *Store) CreateRun(ctx context.Context, plan *storage.RunPlan) error {
	validationErr := storage.ValidateRunPlan(plan)
	if validationErr != nil {
		return fmt.Errorf("create run: %w", validationErr)
	}

	return runTransaction(ctx, s.pool, "create run", func(transaction *sql.Tx) error {
		inserted, err := insertRun(ctx, transaction, &plan.Run, false)
		if err != nil {
			return err
		}

		if !inserted {
			return fmt.Errorf("insert run %q: no row returned", plan.Run.ID)
		}

		if nodesErr := insertNodes(ctx, transaction, plan); nodesErr != nil {
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

		inserted, insertErr := insertRun(ctx, transaction, &plan.Run, true)
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

		if nodesErr := insertNodes(ctx, transaction, plan); nodesErr != nil {
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
) (bool, error) {
	query := `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, output_payload,
		terminal_node_id, error_payload, created_at, updated_at, completed_at,
		max_attempts, retry_base_delay_ns, retry_max_delay_ns, retry_policy_version,
		idempotency_key, submission_fingerprint
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`
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
		run.CreatedAt,
		run.UpdatedAt,
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

func insertNodes(ctx context.Context, transaction *sql.Tx, plan *storage.RunPlan) error {
	const query = `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_owner, lease_generation, lease_expires_at,
		output_payload, error_payload, started_at, completed_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`

	for index := range plan.Nodes {
		node := &plan.Nodes[index]

		_, err := transaction.ExecContext(
			ctx,
			query,
			plan.Run.ID,
			node.ID,
			node.FunctionKey,
			node.SignatureHash,
			node.Status,
			node.RemainingDeps,
			node.Attempt,
			node.AvailableAt,
			nullableString(node.Lease.Owner),
			node.Lease.Generation,
			nullableTime(node.Lease.ExpiresAt),
			nullablePayload(node.Output),
			nullablePayload(node.Error),
			nullableTimePointer(node.StartedAt),
			nullableTimePointer(node.CompletedAt),
		)
		if err != nil {
			return fmt.Errorf("insert node %q for run %q: %w", node.ID, plan.Run.ID, err)
		}
	}

	return nil
}

func insertEdges(ctx context.Context, transaction *sql.Tx, plan *storage.RunPlan) error {
	const query = `INSERT INTO cord_edges (
		run_id, parent_node_id, child_node_id, parent_order
	) VALUES ($1,$2,$3,$4)`

	for _, edge := range plan.Edges {
		_, err := transaction.ExecContext(
			ctx, query, plan.Run.ID, edge.Parent, edge.Child, edge.ParentOrder,
		)
		if err != nil {
			return fmt.Errorf(
				"insert edge %q -> %q for run %q: %w",
				edge.Parent,
				edge.Child,
				plan.Run.ID,
				err,
			)
		}
	}

	return nil
}

func nullablePayload(value storage.EncodedPayload) any {
	if value == nil {
		return nil
	}

	return []byte(value)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}

	return value
}

func nullableStringPointer(value *string) any {
	if value == nil {
		return nil
	}

	return *value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}

	return value
}

func nullableTimePointer(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}

	return *value
}
