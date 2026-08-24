package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

const (
	runsTable  = "cord_runs"
	nodesTable = "cord_nodes"

	schemaV1 = 1
	schemaV2 = 2
	schemaV3 = 3
	schemaV4 = 4
	schemaV5 = 5
)

func migrations() []*goose.Migration {
	return []*goose.Migration{
		goose.NewGoMigration(schemaV1, &goose.GoFunc{RunTx: migrateV1}, nil),
		goose.NewGoMigration(schemaV2, &goose.GoFunc{RunTx: migrateV2}, nil),
		goose.NewGoMigration(schemaV3, &goose.GoFunc{RunTx: migrateV3}, nil),
		goose.NewGoMigration(schemaV4, &goose.GoFunc{RunTx: migrateV4}, nil),
		goose.NewGoMigration(schemaV5, &goose.GoFunc{RunTx: migrateV5}, nil),
	}
}

func migrateV5(ctx context.Context, transaction *sql.Tx) error {
	columns := []struct {
		table, name, statement string
	}{
		{runsTable, "started_at", "ALTER TABLE cord_runs ADD COLUMN started_at TEXT"},
		{runsTable, "terminal_reason", "ALTER TABLE cord_runs ADD COLUMN terminal_reason TEXT"},
		{runsTable, "terminal_runner_id", "ALTER TABLE cord_runs ADD COLUMN terminal_runner_id TEXT"},
		{nodesTable, "state_changed_at", "ALTER TABLE cord_nodes ADD COLUMN state_changed_at TEXT"},
		{nodesTable, "last_started_at", "ALTER TABLE cord_nodes ADD COLUMN last_started_at TEXT"},
		{nodesTable, "last_runner_id", "ALTER TABLE cord_nodes ADD COLUMN last_runner_id TEXT"},
		{nodesTable, "terminal_reason", "ALTER TABLE cord_nodes ADD COLUMN terminal_reason TEXT"},
	}

	for _, column := range columns {
		exists, err := columnExists(ctx, transaction, column.table, column.name)
		if err != nil {
			return err
		}

		if exists {
			continue
		}

		if _, err = transaction.ExecContext(ctx, column.statement); err != nil {
			return fmt.Errorf("add lifecycle column %q.%q: %w", column.table, column.name, err)
		}
	}

	return nil
}

func migrateV4(ctx context.Context, transaction *sql.Tx) error {
	columns := []struct {
		name      string
		statement string
	}{
		{name: "idempotency_key", statement: "ALTER TABLE cord_runs ADD COLUMN idempotency_key TEXT"},
		{
			name:      "submission_fingerprint",
			statement: "ALTER TABLE cord_runs ADD COLUMN submission_fingerprint TEXT",
		},
	}

	for _, column := range columns {
		exists, err := columnExists(ctx, transaction, "cord_runs", column.name)
		if err != nil {
			return err
		}

		if !exists {
			if _, err = transaction.ExecContext(ctx, column.statement); err != nil {
				return fmt.Errorf("add submission identity column %q: %w", column.name, err)
			}
		}
	}

	_, err := transaction.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS
		cord_runs_workflow_name_idempotency_key_idx
		ON cord_runs(workflow_name, idempotency_key)`)
	if err != nil {
		return fmt.Errorf("create run idempotency index: %w", err)
	}

	return nil
}

func migrateV3(ctx context.Context, transaction *sql.Tx) error {
	_, err := transaction.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS cord_edges_run_child_parent_order_idx
		ON cord_edges(run_id, child_node_id, parent_order)`)
	if err != nil {
		return fmt.Errorf("create ordered-child edge index: %w", err)
	}

	return nil
}

func migrateV2(ctx context.Context, transaction *sql.Tx) error {
	columns := []struct {
		name      string
		statement string
	}{
		{
			name:      "max_attempts",
			statement: "ALTER TABLE cord_runs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3",
		},
		{
			name:      "retry_base_delay_ns",
			statement: "ALTER TABLE cord_runs ADD COLUMN retry_base_delay_ns INTEGER NOT NULL DEFAULT 500000000",
		},
		{
			name:      "retry_max_delay_ns",
			statement: "ALTER TABLE cord_runs ADD COLUMN retry_max_delay_ns INTEGER NOT NULL DEFAULT 30000000000",
		},
		{
			name:      "retry_policy_version",
			statement: "ALTER TABLE cord_runs ADD COLUMN retry_policy_version INTEGER NOT NULL DEFAULT 1",
		},
	}

	for _, column := range columns {
		exists, err := columnExists(ctx, transaction, "cord_runs", column.name)
		if err != nil {
			return err
		}

		if exists {
			continue
		}

		if _, err = transaction.ExecContext(ctx, column.statement); err != nil {
			return fmt.Errorf("add persisted retry policy column %q: %w", column.name, err)
		}
	}

	return nil
}

func columnExists(ctx context.Context, transaction *sql.Tx, table, column string) (bool, error) {
	var exists bool

	err := transaction.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pragma_table_info(?) WHERE name = ?)", table, column,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("inspect sqlite column %q.%q: %w", table, column, err)
	}

	return exists, nil
}

func migrateV1(ctx context.Context, transaction *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS cord_runs (
			id TEXT PRIMARY KEY,
			workflow_name TEXT NOT NULL,
			definition_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			input_payload BLOB NOT NULL,
			output_payload BLOB,
			terminal_node_id TEXT NOT NULL,
			error_payload BLOB,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			completed_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS cord_nodes (
			run_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			function_key TEXT NOT NULL,
			signature_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			remaining_deps INTEGER NOT NULL,
			attempt INTEGER NOT NULL,
			available_at TEXT NOT NULL,
			lease_owner TEXT,
			lease_generation INTEGER NOT NULL,
			lease_expires_at TEXT,
			output_payload BLOB,
			error_payload BLOB,
			started_at TEXT,
			completed_at TEXT,
			PRIMARY KEY (run_id, node_id),
			FOREIGN KEY (run_id) REFERENCES cord_runs(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS cord_edges (
			run_id TEXT NOT NULL,
			parent_node_id TEXT NOT NULL,
			child_node_id TEXT NOT NULL,
			parent_order INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (run_id, parent_node_id, child_node_id),
			FOREIGN KEY (run_id, parent_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE,
			FOREIGN KEY (run_id, child_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE
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
			return fmt.Errorf("execute initial sqlite schema statement: %w", err)
		}
	}

	return nil
}
