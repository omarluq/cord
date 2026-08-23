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
type Store struct{ database *sql.DB }

var _ storage.Backend = (*Store)(nil)

// New creates a PostgreSQL store for a caller-owned SQL database.
func New(database *sql.DB) (*Store, error) {
	if database == nil {
		return nil, errors.New("create postgres store: database is nil")
	}

	return &Store{database: database}, nil
}

// CreateRun atomically persists a complete run plan.
func (s *Store) CreateRun(ctx context.Context, plan *storage.RunPlan) error {
	if err := storage.ValidateRunPlan(plan); err != nil {
		return fmt.Errorf("create run: %w", err)
	}

	return runTransaction(ctx, s.database, "create run", func(transaction *sql.Tx) error {
		if err := insertRun(ctx, transaction, &plan.Run); err != nil {
			return err
		}

		if err := insertNodes(ctx, transaction, plan); err != nil {
			return err
		}

		return insertEdges(ctx, transaction, plan)
	})
}

func insertRun(ctx context.Context, transaction *sql.Tx, run *storage.Run) error {
	const query = `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, output_payload,
		terminal_node_id, error_payload, created_at, updated_at, completed_at,
		max_attempts, retry_base_delay_ns, retry_max_delay_ns, retry_policy_version
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`

	_, err := transaction.ExecContext(
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
		run.CompletedAt,
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
			node.StartedAt,
			node.CompletedAt,
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

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}

	return value
}
