package storage

type adapter interface {
	insertRunStatement() string
	insertNodeStatement() string
	insertEdgeStatement() string
}

type sqliteAdapter struct{}

func (sqliteAdapter) insertRunStatement() string {
	return `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, output_payload,
		terminal_node_id, error_payload, created_at, updated_at, completed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
}

func (sqliteAdapter) insertNodeStatement() string {
	return `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_owner, lease_generation, lease_expires_at,
		output_payload, error_payload, started_at, completed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
}

func (sqliteAdapter) insertEdgeStatement() string {
	return `INSERT INTO cord_edges (run_id, parent_node_id, child_node_id) VALUES (?, ?, ?)`
}
