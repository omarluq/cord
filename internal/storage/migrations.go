// Package storage implements Cord's private SQL state machine.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
)

const (
	schemaVersionTable = "cord_schema_migrations"
	requiredVersion    = int64(1)
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
	const (
		attempts = 20
		delay    = 10 * time.Millisecond
	)

	for attempt := range attempts {
		provider, err := newProvider(database)
		if err != nil {
			return fmt.Errorf("create migration provider: %w", err)
		}

		if _, err = provider.Up(ctx); err == nil {
			return nil
		} else if !strings.Contains(err.Error(), "SQLITE_BUSY") || attempt == attempts-1 {
			return fmt.Errorf("apply migrations: %w", err)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait to retry migration: %w", ctx.Err())
		case <-time.After(delay):
		}
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
