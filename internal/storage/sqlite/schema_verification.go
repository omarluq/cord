package sqlite

const (
	affinityText    = "TEXT"
	affinityBlob    = "BLOB"
	affinityInteger = "INTEGER"
	onDeleteCascade = "CASCADE"
	runIDColumn     = "run_id"
	doubleTypeToken = "DO" + "UB"
	secondKey       = 2
	thirdKey        = 3
)

type schemaColumn struct {
	name         string
	declaredType string
	affinity     string
	defaultSQL   string
	notNull      bool
	primaryKey   int
}

type schemaIndex struct {
	name    string
	columns []string
	unique  bool
	partial bool
}

type schemaForeignKey struct {
	table    string
	onDelete string
	from     []string
	to       []string
}

type schemaTable struct {
	name        string
	columns     []schemaColumn
	indexes     []schemaIndex
	foreignKeys []schemaForeignKey
}

func column(name, affinity string, notNull bool, defaultSQL string, primaryKey int) schemaColumn {
	return schemaColumn{
		name: name, declaredType: "", affinity: affinity, defaultSQL: defaultSQL,
		notNull: notNull, primaryKey: primaryKey,
	}
}

func lifecycleColumn(name string) schemaColumn {
	return schemaColumn{
		name: name, declaredType: affinityText, affinity: affinityText,
		notNull: false, defaultSQL: "", primaryKey: 0,
	}
}

func index(name string, columns ...string) schemaIndex {
	return schemaIndex{name: name, columns: columns, unique: false, partial: false}
}

func uniqueIndex(name string, columns ...string) schemaIndex {
	return schemaIndex{name: name, columns: columns, unique: true, partial: false}
}

func foreignKey(table string, from, to []string) schemaForeignKey {
	return schemaForeignKey{table: table, from: from, to: to, onDelete: onDeleteCascade}
}

func runColumns() []schemaColumn {
	return []schemaColumn{
		column("id", affinityText, false, "", 1),
		column("workflow_name", affinityText, true, "", 0),
		column("definition_hash", affinityText, true, "", 0),
		column("status", affinityText, true, "", 0),
		column("input_payload", affinityBlob, true, "", 0),
		column("output_payload", affinityBlob, false, "", 0),
		column("terminal_node_id", affinityText, true, "", 0),
		column("error_payload", affinityBlob, false, "", 0),
		column("created_at", affinityText, true, "", 0),
		column("updated_at", affinityText, true, "", 0),
		column("completed_at", affinityText, false, "", 0),
		column("max_attempts", affinityInteger, true, "3", 0),
		column("retry_base_delay_ns", affinityInteger, true, "500000000", 0),
		column("retry_max_delay_ns", affinityInteger, true, "30000000000", 0),
		column("retry_policy_version", affinityInteger, true, "1", 0),
		column("idempotency_key", affinityText, false, "", 0),
		column("submission_fingerprint", affinityText, false, "", 0),
		lifecycleColumn("started_at"),
		lifecycleColumn("terminal_reason"),
		lifecycleColumn("terminal_runner_id"),
	}
}

func nodeColumns() []schemaColumn {
	return []schemaColumn{
		column(runIDColumn, affinityText, true, "", 1),
		column("node_id", affinityText, true, "", secondKey),
		column("function_key", affinityText, true, "", 0),
		column("signature_hash", affinityText, true, "", 0),
		column("status", affinityText, true, "", 0),
		column("remaining_deps", affinityInteger, true, "", 0),
		column("attempt", affinityInteger, true, "", 0),
		column("available_at", affinityText, true, "", 0),
		column("lease_owner", affinityText, false, "", 0),
		column("lease_generation", affinityInteger, true, "", 0),
		column("lease_expires_at", affinityText, false, "", 0),
		column("output_payload", affinityBlob, false, "", 0),
		column("error_payload", affinityBlob, false, "", 0),
		column("started_at", affinityText, false, "", 0),
		column("completed_at", affinityText, false, "", 0),
		lifecycleColumn("state_changed_at"),
		lifecycleColumn("last_started_at"),
		lifecycleColumn("last_runner_id"),
		lifecycleColumn("terminal_reason"),
	}
}

func requiredSchema() []schemaTable {
	return []schemaTable{
		{
			name:    runsTable,
			columns: runColumns(),
			indexes: []schemaIndex{
				uniqueIndex(
					"cord_runs_workflow_name_idempotency_key_idx",
					"workflow_name",
					"idempotency_key",
				),
			},
			foreignKeys: nil,
		},
		{
			name:    nodesTable,
			columns: nodeColumns(),
			indexes: []schemaIndex{
				index("cord_nodes_status_available_at_idx", "status", "available_at"),
				index("cord_nodes_function_status_available_at_idx", "function_key", "status", "available_at"),
				index("cord_nodes_run_status_idx", "run_id", "status"),
				index("cord_nodes_lease_expires_at_idx", "lease_expires_at"),
			},
			foreignKeys: []schemaForeignKey{
				foreignKey("cord_runs", []string{runIDColumn}, []string{"id"}),
			},
		},
		{
			name: "cord_edges",
			columns: []schemaColumn{
				column(runIDColumn, affinityText, true, "", 1),
				column("parent_node_id", affinityText, true, "", secondKey),
				column("child_node_id", affinityText, true, "", thirdKey),
				column("parent_order", affinityInteger, true, "0", 0),
			},
			indexes: []schemaIndex{
				index("cord_edges_run_child_parent_order_idx", runIDColumn, "child_node_id", "parent_order"),
			},
			foreignKeys: []schemaForeignKey{
				foreignKey("cord_nodes", []string{runIDColumn, "child_node_id"}, []string{runIDColumn, "node_id"}),
				foreignKey("cord_nodes", []string{runIDColumn, "parent_node_id"}, []string{runIDColumn, "node_id"}),
			},
		},
	}
}
