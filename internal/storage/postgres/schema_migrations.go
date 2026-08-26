package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

func migrations() []*goose.Migration {
	return []*goose.Migration{
		goose.NewGoMigration(initialVersion, &goose.GoFunc{RunTx: migrateV1}, nil),
		goose.NewGoMigration(parentOrderVersion, &goose.GoFunc{RunTx: migrateV2}, nil),
		goose.NewGoMigration(idempotencyVersion, &goose.GoFunc{RunTx: migrateV3}, nil),
		goose.NewGoMigration(requiredVersion, &goose.GoFunc{RunTx: migrateV4}, nil),
	}
}

func migrateV1(ctx context.Context, transaction *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS cord_runs (
			id TEXT PRIMARY KEY,
			workflow_name TEXT NOT NULL,
			definition_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			input_payload BYTEA NOT NULL,
			output_payload BYTEA,
			terminal_node_id TEXT NOT NULL,
			error_payload BYTEA,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			completed_at TIMESTAMPTZ,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			retry_base_delay_ns BIGINT NOT NULL DEFAULT 500000000,
			retry_max_delay_ns BIGINT NOT NULL DEFAULT 30000000000,
			retry_policy_version INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE IF NOT EXISTS cord_nodes (
			run_id TEXT NOT NULL REFERENCES cord_runs(id) ON DELETE CASCADE,
			node_id TEXT NOT NULL,
			function_key TEXT NOT NULL,
			signature_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			remaining_deps INTEGER NOT NULL,
			attempt INTEGER NOT NULL,
			available_at TIMESTAMPTZ NOT NULL,
			lease_owner TEXT,
			lease_generation BIGINT NOT NULL,
			lease_expires_at TIMESTAMPTZ,
			output_payload BYTEA,
			error_payload BYTEA,
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			PRIMARY KEY (run_id, node_id)
		)`,
		`CREATE TABLE IF NOT EXISTS cord_edges (
			run_id TEXT NOT NULL,
			parent_node_id TEXT NOT NULL,
			child_node_id TEXT NOT NULL,
			parent_order INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (run_id, parent_node_id, child_node_id),
			FOREIGN KEY (run_id, parent_node_id)
				REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE,
			FOREIGN KEY (run_id, child_node_id)
				REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS cord_nodes_status_available_at_idx
			ON cord_nodes(status, available_at)`,
		`CREATE INDEX IF NOT EXISTS cord_nodes_function_status_available_at_idx
			ON cord_nodes(function_key, status, available_at)`,
		`CREATE INDEX IF NOT EXISTS cord_nodes_run_status_idx
			ON cord_nodes(run_id, status)`,
		`CREATE INDEX IF NOT EXISTS cord_nodes_lease_expires_at_idx
			ON cord_nodes(lease_expires_at)`,
	}

	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute initial postgres schema statement: %w", err)
		}
	}

	return nil
}

func migrateV2(ctx context.Context, transaction *sql.Tx) error {
	const statement = `CREATE INDEX IF NOT EXISTS cord_edges_run_child_parent_order_idx
		ON cord_edges(run_id, child_node_id, parent_order)`
	if _, err := transaction.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("create parent-output lookup index: %w", err)
	}

	return nil
}

func migrateV3(ctx context.Context, transaction *sql.Tx) error {
	// Adding nullable columns without defaults is a metadata-only operation on
	// supported PostgreSQL versions and preserves all pre-v3 run rows. Goose's
	// transactional Go migration pattern precludes CREATE INDEX CONCURRENTLY;
	// create the unique index only after both inexpensive column additions.
	statements := []string{
		`ALTER TABLE cord_runs ADD COLUMN IF NOT EXISTS idempotency_key TEXT`,
		`ALTER TABLE cord_runs ADD COLUMN IF NOT EXISTS submission_fingerprint TEXT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS cord_runs_workflow_name_idempotency_key_idx
			ON cord_runs(workflow_name, idempotency_key)`,
	}

	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("add idempotent submission schema: %w", err)
		}
	}

	return nil
}

func migrateV4(ctx context.Context, transaction *sql.Tx) error {
	statements := []string{
		`ALTER TABLE cord_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ`,
		`ALTER TABLE cord_runs ADD COLUMN IF NOT EXISTS terminal_reason TEXT`,
		`ALTER TABLE cord_runs ADD COLUMN IF NOT EXISTS terminal_runner_id TEXT`,
		`ALTER TABLE cord_nodes ADD COLUMN IF NOT EXISTS state_changed_at TIMESTAMPTZ`,
		`ALTER TABLE cord_nodes ADD COLUMN IF NOT EXISTS last_started_at TIMESTAMPTZ`,
		`ALTER TABLE cord_nodes ADD COLUMN IF NOT EXISTS last_runner_id TEXT`,
		`ALTER TABLE cord_nodes ADD COLUMN IF NOT EXISTS terminal_reason TEXT`,
	}

	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("add lifecycle snapshot schema: %w", err)
		}
	}

	return nil
}
