package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

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
