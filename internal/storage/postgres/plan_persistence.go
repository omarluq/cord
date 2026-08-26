package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func insertNodes(
	ctx context.Context,
	transaction *sql.Tx,
	plan *storage.RunPlan,
	createdAt time.Time,
) error {
	const query = `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_owner, lease_generation, lease_expires_at,
		output_payload, error_payload, started_at, completed_at,
		state_changed_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`

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
			createdAt,
		)
		if err != nil {
			return fmt.Errorf("insert node %q for run %q: %w", node.ID, plan.Run.ID, err)
		}
	}

	return nil
}

func databaseInstant(ctx context.Context, transaction *sql.Tx) (time.Time, error) {
	var instant time.Time
	if err := transaction.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&instant); err != nil {
		return time.Time{}, fmt.Errorf("read database time: %w", err)
	}

	return instant.UTC(), nil
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
