package sqlite

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

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

func verifySchemaStructure(ctx context.Context, database *sql.DB) error {
	tables := requiredSchema()
	for position := range tables {
		if err := verifyTable(ctx, database, &tables[position]); err != nil {
			return fmt.Errorf("schema is incompatible: %w", err)
		}
	}

	return nil
}

func verifyTable(ctx context.Context, database *sql.DB, expected *schemaTable) error {
	if err := verifyTableObject(ctx, database, expected.name); err != nil {
		return err
	}

	if err := verifyColumns(ctx, database, expected); err != nil {
		return err
	}

	if err := verifyIndexes(ctx, database, expected); err != nil {
		return err
	}

	return verifyForeignKeys(ctx, database, expected)
}

func verifyTableObject(ctx context.Context, database *sql.DB, name string) error {
	var objectType string

	err := database.QueryRowContext(ctx, "SELECT type FROM sqlite_schema WHERE name = ?", name).Scan(&objectType)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("table %q is missing", name)
	}

	if err != nil {
		return fmt.Errorf("inspect schema object %q: %w", name, err)
	}

	if objectType != "table" {
		return fmt.Errorf("schema object %q has type %q; want table", name, objectType)
	}

	return nil
}

func verifyColumns(ctx context.Context, database *sql.DB, expected *schemaTable) error {
	columns, err := inspectColumns(ctx, database, expected.name)
	if err != nil {
		return err
	}

	columnsByName := make(map[string]schemaColumn, len(columns))
	for _, current := range columns {
		columnsByName[current.name] = current
	}

	for _, wanted := range expected.columns {
		current, exists := columnsByName[wanted.name]
		if !exists {
			return fmt.Errorf("column %q.%q is missing", expected.name, wanted.name)
		}

		if !columnsEqual(&current, &wanted) {
			return fmt.Errorf(
				"column %q.%q is %s; want %s",
				expected.name, wanted.name, describeColumn(&current), describeColumn(&wanted),
			)
		}
	}

	return nil
}

func verifyIndexes(ctx context.Context, database *sql.DB, expected *schemaTable) error {
	indexes, err := inspectIndexes(ctx, database, expected.name)
	if err != nil {
		return err
	}

	for _, wanted := range expected.indexes {
		current, exists := indexes[wanted.name]
		if !exists {
			return fmt.Errorf("index %q is missing", wanted.name)
		}

		if current.unique != wanted.unique || current.partial != wanted.partial ||
			!equalStrings(current.columns, wanted.columns) {
			return fmt.Errorf(
				"index %q has columns %v, unique=%t, and partial=%t; want columns %v, unique=%t, and partial=%t",
				wanted.name, current.columns, current.unique, current.partial,
				wanted.columns, wanted.unique, wanted.partial,
			)
		}
	}

	return nil
}

func verifyForeignKeys(ctx context.Context, database *sql.DB, expected *schemaTable) error {
	foreignKeys, err := inspectForeignKeys(ctx, database, expected.name)
	if err != nil {
		return err
	}

	for position := range expected.foreignKeys {
		if !containsForeignKey(foreignKeys, &expected.foreignKeys[position]) {
			return fmt.Errorf(
				"table %q is missing foreign key %v; found %v",
				expected.name, expected.foreignKeys[position], foreignKeys,
			)
		}
	}

	return nil
}

func inspectColumns(ctx context.Context, database *sql.DB, table string) (columns []schemaColumn, err error) {
	const query = `SELECT name, type, "notnull", dflt_value, pk FROM pragma_table_info(?) ORDER BY cid`

	rows, err := database.QueryContext(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("inspect columns for table %q: %w", table, err)
	}

	defer func() { err = errors.Join(err, rows.Close()) }()

	for rows.Next() {
		var (
			current    schemaColumn
			declared   string
			defaultSQL sql.NullString
		)
		if err = rows.Scan(&current.name, &declared, &current.notNull, &defaultSQL, &current.primaryKey); err != nil {
			return nil, fmt.Errorf("scan columns for table %q: %w", table, err)
		}

		current.declaredType = strings.ToUpper(strings.TrimSpace(declared))

		current.affinity = sqliteAffinity(declared)
		if defaultSQL.Valid {
			current.defaultSQL = normalizeDefault(defaultSQL.String)
		}

		columns = append(columns, current)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns for table %q: %w", table, err)
	}

	return columns, nil
}

func inspectIndexes(ctx context.Context, database *sql.DB, table string) (map[string]schemaIndex, error) {
	indexes, err := inspectIndexList(ctx, database, table)
	if err != nil {
		return nil, err
	}

	for name, current := range indexes {
		columns, columnsErr := inspectIndexColumns(ctx, database, name)
		if columnsErr != nil {
			return nil, columnsErr
		}

		current.columns = columns
		indexes[name] = current
	}

	return indexes, nil
}

func inspectIndexList(ctx context.Context, database *sql.DB, table string) (indexes map[string]schemaIndex, err error) {
	rows, err := database.QueryContext(ctx, `SELECT name, "unique", partial FROM pragma_index_list(?)`, table)
	if err != nil {
		return nil, fmt.Errorf("inspect indexes for table %q: %w", table, err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()

	indexes = make(map[string]schemaIndex)

	for rows.Next() {
		var current schemaIndex
		if err = rows.Scan(&current.name, &current.unique, &current.partial); err != nil {
			return nil, fmt.Errorf("scan indexes for table %q: %w", table, err)
		}

		indexes[current.name] = current
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexes for table %q: %w", table, err)
	}

	return indexes, nil
}

func inspectIndexColumns(ctx context.Context, database *sql.DB, name string) (columns []string, err error) {
	rows, err := database.QueryContext(ctx, "SELECT name FROM pragma_index_info(?) ORDER BY seqno", name)
	if err != nil {
		return nil, fmt.Errorf("inspect index %q: %w", name, err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()

	for rows.Next() {
		var current string
		if err = rows.Scan(&current); err != nil {
			return nil, fmt.Errorf("scan index %q: %w", name, err)
		}

		columns = append(columns, current)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate index %q: %w", name, err)
	}

	return columns, nil
}

func inspectForeignKeys(
	ctx context.Context,
	database *sql.DB,
	table string,
) (foreignKeys []schemaForeignKey, err error) {
	const query = `SELECT id, seq, "table", "from", "to", on_delete
		FROM pragma_foreign_key_list(?) ORDER BY id, seq`

	rows, err := database.QueryContext(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("inspect foreign keys for table %q: %w", table, err)
	}

	defer func() { err = errors.Join(err, rows.Close()) }()

	byID := make(map[int]*schemaForeignKey)

	var identifiers []int

	for rows.Next() {
		var (
			identifier, sequence                            int
			referencedTable, fromColumn, toColumn, onDelete string
		)
		if err = rows.Scan(&identifier, &sequence, &referencedTable, &fromColumn, &toColumn, &onDelete); err != nil {
			return nil, fmt.Errorf("scan foreign keys for table %q: %w", table, err)
		}

		current := byID[identifier]
		if current == nil {
			current = &schemaForeignKey{table: referencedTable, from: nil, to: nil, onDelete: strings.ToUpper(onDelete)}
			byID[identifier] = current
			identifiers = append(identifiers, identifier)
		}

		current.from = append(current.from, fromColumn)
		current.to = append(current.to, toColumn)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate foreign keys for table %q: %w", table, err)
	}

	foreignKeys = make([]schemaForeignKey, 0, len(identifiers))
	for _, identifier := range identifiers {
		foreignKeys = append(foreignKeys, *byID[identifier])
	}

	slices.SortFunc(foreignKeys, func(left, right schemaForeignKey) int {
		return compareForeignKeys(&left, &right)
	})

	return foreignKeys, nil
}

func compareForeignKeys(left, right *schemaForeignKey) int {
	if compared := cmp.Compare(left.table, right.table); compared != 0 {
		return compared
	}

	if compared := cmp.Compare(left.onDelete, right.onDelete); compared != 0 {
		return compared
	}

	if compared := slices.Compare(left.from, right.from); compared != 0 {
		return compared
	}

	return slices.Compare(left.to, right.to)
}

func sqliteAffinity(declaredType string) string {
	declaredType = strings.ToUpper(declaredType)
	switch {
	case strings.Contains(declaredType, "INT"):
		return affinityInteger
	case containsAny(declaredType, "CHAR", "CLOB", affinityText):
		return affinityText
	case declaredType == "", strings.Contains(declaredType, affinityBlob):
		return affinityBlob
	case containsAny(declaredType, "REAL", "FLOA", doubleTypeToken):
		return "REAL"
	default:
		return "NUMERIC"
	}
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}

	return false
}

func normalizeDefault(value string) string {
	value = strings.TrimSpace(value)
	for len(value) >= 2 && value[0] == '(' && value[len(value)-1] == ')' {
		value = strings.TrimSpace(value[1 : len(value)-1])
	}

	if integer, err := strconv.ParseInt(value, 10, 64); err == nil {
		return strconv.FormatInt(integer, 10)
	}

	return value
}

func columnsEqual(current, wanted *schemaColumn) bool {
	return current.affinity == wanted.affinity && current.defaultSQL == wanted.defaultSQL &&
		current.notNull == wanted.notNull && current.primaryKey == wanted.primaryKey &&
		(wanted.declaredType == "" || current.declaredType == wanted.declaredType)
}

func describeColumn(current *schemaColumn) string {
	return fmt.Sprintf(
		"declared-type=%q affinity=%s not-null=%t default=%q primary-key-position=%d",
		current.declaredType, current.affinity, current.notNull, current.defaultSQL, current.primaryKey,
	)
}

func equalStrings(left, right []string) bool {
	return strings.Join(left, "\x00") == strings.Join(right, "\x00")
}

func containsForeignKey(foreignKeys []schemaForeignKey, expected *schemaForeignKey) bool {
	for _, current := range foreignKeys {
		if current.table == expected.table && current.onDelete == expected.onDelete &&
			equalStrings(current.from, expected.from) && equalStrings(current.to, expected.to) {
			return true
		}
	}

	return false
}
