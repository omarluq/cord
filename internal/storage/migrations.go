// Package storage implements Cord's private SQL state machine.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const (
	schemaVersionTable = "cord_schema_migrations"
	requiredVersion    = int64(2)
)

var (
	// ErrSchemaOutdated indicates that the schema is absent or too old.
	ErrSchemaOutdated = errors.New("schema is absent or outdated")
	// ErrSchemaNewer indicates that the schema is newer than this runtime.
	ErrSchemaNewer = errors.New("schema is newer than runtime")
)

// Verify checks SQLite schema compatibility without executing DDL.
func Verify(ctx context.Context, database *sql.DB) error {
	current, exists, err := sqliteSchemaVersion(ctx, database)
	if err != nil {
		return fmt.Errorf("inspect sqlite schema: %w", err)
	}

	if !exists || current < requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", ErrSchemaOutdated, current, requiredVersion)
	}

	if current > requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", ErrSchemaNewer, current, requiredVersion)
	}

	return nil
}

// Migrate applies all pending SQLite migrations.
func Migrate(ctx context.Context, database *sql.DB) error {
	if err := migrateWithRetry(ctx, database); err != nil {
		return err
	}

	if err := Verify(ctx, database); err != nil {
		return fmt.Errorf("verify migrated schema: %w", err)
	}

	return nil
}

func migrateWithRetry(ctx context.Context, database *sql.DB) error {
	const migrationRetryAttempts = 100

	if err := retrySQLiteWithAttempts(
		ctx,
		"initialize migration table",
		migrationRetryAttempts,
		isSQLiteContention,
		func() error { return initializeMigrationTable(ctx, database) },
	); err != nil {
		return fmt.Errorf("initialize migrations: %w", err)
	}

	if err := retrySQLiteWithAttempts(
		ctx,
		"wait to retry migration",
		migrationRetryAttempts,
		isSQLiteContention,
		func() error { return applySQLiteMigrations(ctx, database) },
	); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}

func initializeMigrationTable(ctx context.Context, database *sql.DB) error {
	return withImmediateTransaction(ctx, database, func(connection *sql.Conn) error {
		_, err := connection.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cord_schema_migrations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			version_id INTEGER NOT NULL,
			is_applied INTEGER NOT NULL,
			tstamp TIMESTAMP DEFAULT (datetime('now'))
		)`)
		if err != nil {
			return fmt.Errorf("create migration table: %w", err)
		}

		_, err = connection.ExecContext(ctx, `INSERT INTO cord_schema_migrations (version_id, is_applied)
			SELECT 0, 1 WHERE NOT EXISTS (SELECT 1 FROM cord_schema_migrations)`)
		if err != nil {
			return fmt.Errorf("initialize migration version: %w", err)
		}

		return nil
	})
}

func applySQLiteMigrations(ctx context.Context, database *sql.DB) error {
	return withImmediateTransaction(ctx, database, func(connection *sql.Conn) error {
		current, err := sqliteSchemaVersionFrom(ctx, connection)
		if err != nil {
			return err
		}

		for _, migration := range sqliteMigrations() {
			if migration.version <= current {
				continue
			}

			if err := migration.run(ctx, connection); err != nil {
				return fmt.Errorf("apply sqlite migration %d: %w", migration.version, err)
			}

			if _, err := connection.ExecContext(ctx, `INSERT INTO cord_schema_migrations
				(version_id, is_applied) VALUES (?, 1)`, migration.version); err != nil {
				return fmt.Errorf("record sqlite migration %d: %w", migration.version, err)
			}
		}

		return nil
	})
}

func withImmediateTransaction(
	ctx context.Context,
	database *sql.DB,
	operation func(*sql.Conn) error,
) (returnErr error) {
	connection, err := database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire sqlite connection: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, connection.Close())
	}()

	if _, err := connection.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin immediate sqlite transaction: %w", err)
	}

	defer func() {
		if returnErr != nil {
			_, rollbackErr := connection.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
			if rollbackErr != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("rollback sqlite transaction: %w", rollbackErr))
			}
		}
	}()

	if err := operation(connection); err != nil {
		return err
	}

	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit sqlite transaction: %w", err)
	}

	return nil
}

func sqliteSchemaVersion(ctx context.Context, database *sql.DB) (current int64, exists bool, err error) {
	var tableName string

	err = database.QueryRowContext(
		ctx,
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
		schemaVersionTable,
	).Scan(&tableName)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, fmt.Errorf("query sqlite schema table: %w", err)
	}

	current, err = sqliteSchemaVersionFrom(ctx, database)
	if err != nil {
		return 0, true, err
	}

	return current, true, nil
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func sqliteSchemaVersionFrom(ctx context.Context, queryer rowQueryer) (int64, error) {
	var (
		current int64
		applied bool
	)

	err := queryer.QueryRowContext(
		ctx,
		"SELECT version_id, is_applied FROM cord_schema_migrations ORDER BY id DESC LIMIT 1",
	).Scan(&current, &applied)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("query sqlite schema version: %w", err)
	}

	if !applied {
		current--
	}

	return max(0, current), nil
}
