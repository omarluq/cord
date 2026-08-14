// Package storage implements Cord's private SQL state machine.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/pressly/goose/v3"
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
	provider, err := newProvider(database)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}

	err = retrySQLiteContention(ctx, "wait to retry migration", func() error {
		_, upErr := provider.Up(ctx)
		if upErr != nil {
			return fmt.Errorf("run migration provider: %w", upErr)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}

func newProvider(database *sql.DB) (*goose.Provider, error) {
	provider, err := goose.NewProvider(
		goose.DialectSQLite3,
		database,
		nil,
		goose.WithDisableGlobalRegistry(true),
		goose.WithGoMigrations(sqliteMigrations()...),
		goose.WithTableName(schemaVersionTable),
	)
	if err != nil {
		return nil, fmt.Errorf("configure goose: %w", err)
	}

	return provider, nil
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

	var applied bool

	err = database.QueryRowContext(
		ctx,
		"SELECT version_id, is_applied FROM cord_schema_migrations ORDER BY id DESC LIMIT 1",
	).Scan(&current, &applied)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, true, nil
	}

	if err != nil {
		return 0, true, fmt.Errorf("query sqlite schema version: %w", err)
	}

	if !applied {
		current--
	}

	return max(0, current), true, nil
}
