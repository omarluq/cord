package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

const schemaVerificationQuery = `WITH
	required_tables(name) AS (VALUES
		('cord_schema_migrations'), ('cord_runs'), ('cord_nodes'), ('cord_edges')
	),
	required_columns(table_name, column_name, data_type, not_null, default_sql) AS (VALUES
		('cord_schema_migrations', 'id', 'integer', true, ''),
		('cord_schema_migrations', 'version_id', 'bigint', true, ''),
		('cord_schema_migrations', 'is_applied', 'boolean', true, ''),
		('cord_schema_migrations', 'tstamp', 'timestamp without time zone', true, 'now()'),
		('cord_runs', 'id', 'text', true, ''),
		('cord_runs', 'workflow_name', 'text', true, ''),
		('cord_runs', 'definition_hash', 'text', true, ''),
		('cord_runs', 'status', 'text', true, ''),
		('cord_runs', 'input_payload', 'bytea', true, ''),
		('cord_runs', 'output_payload', 'bytea', false, ''),
		('cord_runs', 'terminal_node_id', 'text', true, ''),
		('cord_runs', 'error_payload', 'bytea', false, ''),
		('cord_runs', 'created_at', 'timestamp with time zone', true, ''),
		('cord_runs', 'updated_at', 'timestamp with time zone', true, ''),
		('cord_runs', 'completed_at', 'timestamp with time zone', false, ''),
		('cord_runs', 'max_attempts', 'integer', true, '3'),
		('cord_runs', 'retry_base_delay_ns', 'bigint', true, '500000000'),
		('cord_runs', 'retry_max_delay_ns', 'bigint', true, '''30000000000''::bigint'),
		('cord_runs', 'retry_policy_version', 'integer', true, '1'),
		('cord_runs', 'idempotency_key', 'text', false, ''),
		('cord_runs', 'submission_fingerprint', 'text', false, ''),
		('cord_runs', 'started_at', 'timestamp with time zone', false, ''),
		('cord_runs', 'terminal_reason', 'text', false, ''),
		('cord_runs', 'terminal_runner_id', 'text', false, ''),
		('cord_nodes', 'run_id', 'text', true, ''),
		('cord_nodes', 'node_id', 'text', true, ''),
		('cord_nodes', 'function_key', 'text', true, ''),
		('cord_nodes', 'signature_hash', 'text', true, ''),
		('cord_nodes', 'status', 'text', true, ''),
		('cord_nodes', 'remaining_deps', 'integer', true, ''),
		('cord_nodes', 'attempt', 'integer', true, ''),
		('cord_nodes', 'available_at', 'timestamp with time zone', true, ''),
		('cord_nodes', 'lease_owner', 'text', false, ''),
		('cord_nodes', 'lease_generation', 'bigint', true, ''),
		('cord_nodes', 'lease_expires_at', 'timestamp with time zone', false, ''),
		('cord_nodes', 'output_payload', 'bytea', false, ''),
		('cord_nodes', 'error_payload', 'bytea', false, ''),
		('cord_nodes', 'started_at', 'timestamp with time zone', false, ''),
		('cord_nodes', 'completed_at', 'timestamp with time zone', false, ''),
		('cord_nodes', 'state_changed_at', 'timestamp with time zone', false, ''),
		('cord_nodes', 'last_started_at', 'timestamp with time zone', false, ''),
		('cord_nodes', 'last_runner_id', 'text', false, ''),
		('cord_nodes', 'terminal_reason', 'text', false, ''),
		('cord_edges', 'run_id', 'text', true, ''),
		('cord_edges', 'parent_node_id', 'text', true, ''),
		('cord_edges', 'child_node_id', 'text', true, ''),
		('cord_edges', 'parent_order', 'integer', true, '0')
	),
	required_primary_keys(table_name, columns) AS (VALUES
		('cord_schema_migrations', ARRAY['id']::text[]),
		('cord_runs', ARRAY['id']::text[]),
		('cord_nodes', ARRAY['run_id', 'node_id']::text[]),
		('cord_edges', ARRAY['run_id', 'parent_node_id', 'child_node_id']::text[])
	),
	required_indexes(index_name, table_name, columns, is_unique) AS (VALUES
		('cord_nodes_status_available_at_idx', 'cord_nodes', ARRAY['status', 'available_at']::text[], false),
		('cord_nodes_function_status_available_at_idx', 'cord_nodes',
			ARRAY['function_key', 'status', 'available_at']::text[], false),
		('cord_nodes_run_status_idx', 'cord_nodes', ARRAY['run_id', 'status']::text[], false),
		('cord_nodes_lease_expires_at_idx', 'cord_nodes', ARRAY['lease_expires_at']::text[], false),
		('cord_edges_run_child_parent_order_idx', 'cord_edges',
			ARRAY['run_id', 'child_node_id', 'parent_order']::text[], false),
		('cord_runs_workflow_name_idempotency_key_idx', 'cord_runs',
			ARRAY['workflow_name', 'idempotency_key']::text[], true)
	),
	required_foreign_keys(source_table, source_columns, target_table, target_columns) AS (VALUES
		('cord_nodes', ARRAY['run_id']::text[], 'cord_runs', ARRAY['id']::text[]),
		('cord_edges', ARRAY['run_id', 'parent_node_id']::text[], 'cord_nodes', ARRAY['run_id', 'node_id']::text[]),
		('cord_edges', ARRAY['run_id', 'child_node_id']::text[], 'cord_nodes', ARRAY['run_id', 'node_id']::text[])
	)
	SELECT
		(SELECT count(*) FROM required_tables rt WHERE NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE n.oid = pg_catalog.current_schema()::regnamespace
				AND c.relname = rt.name AND c.relkind = 'r'
		)) +
		(SELECT count(*) FROM required_columns rc WHERE NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			WHERE n.oid = pg_catalog.current_schema()::regnamespace
				AND c.relname = rc.table_name AND a.attname = rc.column_name
				AND NOT a.attisdropped
				AND pg_catalog.format_type(a.atttypid, a.atttypmod) = rc.data_type
				AND a.attnotnull = rc.not_null
				AND COALESCE(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '') = rc.default_sql
		)) +
		(SELECT count(*) FROM required_primary_keys rp WHERE NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_constraint con
			JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE n.oid = pg_catalog.current_schema()::regnamespace
				AND c.relname = rp.table_name AND con.contype = 'p'
				AND ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY key(attnum, position)
					JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = key.attnum
					ORDER BY key.position) = rp.columns::text[]
		)) +
		(SELECT count(*) FROM required_indexes ri WHERE NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_index i
			JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
			JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = tc.relnamespace
			WHERE n.oid = pg_catalog.current_schema()::regnamespace
				AND ic.relname = ri.index_name AND tc.relname = ri.table_name
				AND i.indisunique = ri.is_unique AND i.indpred IS NULL AND i.indexprs IS NULL
				AND i.indnkeyatts = cardinality(ri.columns)
				AND ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY key(attnum, position)
					JOIN pg_catalog.pg_attribute a ON a.attrelid = tc.oid AND a.attnum = key.attnum
					WHERE key.position <= i.indnkeyatts ORDER BY key.position) = ri.columns::text[]
		)) +
		(SELECT count(*) FROM required_foreign_keys rf WHERE NOT EXISTS (
			SELECT 1 FROM pg_catalog.pg_constraint con
			JOIN pg_catalog.pg_class source ON source.oid = con.conrelid
			JOIN pg_catalog.pg_class target ON target.oid = con.confrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = source.relnamespace
			WHERE n.oid = pg_catalog.current_schema()::regnamespace
				AND con.contype = 'f' AND con.confdeltype = 'c'
				AND source.relname = rf.source_table AND target.relname = rf.target_table
				AND ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY key(attnum, position)
					JOIN pg_catalog.pg_attribute a ON a.attrelid = source.oid AND a.attnum = key.attnum
					ORDER BY key.position) = rf.source_columns::text[]
				AND ARRAY(SELECT a.attname::text FROM unnest(con.confkey) WITH ORDINALITY key(attnum, position)
					JOIN pg_catalog.pg_attribute a ON a.attrelid = target.oid AND a.attnum = key.attnum
					ORDER BY key.position) = rf.target_columns::text[]
		)) AS incompatibilities`

func verifySchemaStructure(ctx context.Context, database *sql.DB) error {
	var incompatibilities int
	if err := database.QueryRowContext(ctx, schemaVerificationQuery).Scan(&incompatibilities); err != nil {
		return fmt.Errorf("inspect postgres schema structure: %w", err)
	}

	if incompatibilities != 0 {
		return fmt.Errorf(
			"%w: PostgreSQL schema has %d incompatible required structures",
			storage.ErrSchemaOutdated,
			incompatibilities,
		)
	}

	return nil
}
