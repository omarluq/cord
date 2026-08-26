package sqlite

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

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
