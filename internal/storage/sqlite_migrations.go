package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	sqliteSchemaV1 = 1
	sqliteSchemaV2 = 2
)

type sqliteMigration struct {
	run     func(context.Context, *sql.Conn) error
	version int64
}

func sqliteMigrations() []sqliteMigration {
	return []sqliteMigration{
		{version: sqliteSchemaV1, run: migrateSQLiteV1},
		{version: sqliteSchemaV2, run: migrateSQLiteV2},
	}
}

func migrateSQLiteV2(ctx context.Context, connection *sql.Conn) error {
	statements := []string{
		`ALTER TABLE cord_runs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3`,
		`ALTER TABLE cord_runs ADD COLUMN retry_base_delay_ns INTEGER NOT NULL DEFAULT 500000000`,
		`ALTER TABLE cord_runs ADD COLUMN retry_max_delay_ns INTEGER NOT NULL DEFAULT 30000000000`,
		`ALTER TABLE cord_runs ADD COLUMN retry_policy_version INTEGER NOT NULL DEFAULT 1`,
	}

	for _, statement := range statements {
		if _, err := connection.ExecContext(ctx, statement); err != nil &&
			!strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add persisted retry policy column: %w", err)
		}
	}

	return nil
}

func migrateSQLiteV1(ctx context.Context, connection *sql.Conn) error {
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
		if _, err := connection.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute initial sqlite schema statement: %w", err)
		}
	}

	return nil
}
