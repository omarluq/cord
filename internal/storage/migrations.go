// Package storage implements Cord's private durable SQL state machine.
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

// Dialect identifies an internal SQL implementation.
type Dialect uint8

const (
	dialectSQLite Dialect = iota + 1
	dialectPostgres
	dialectMySQL
)

var (
	// ErrUnsupportedDialect indicates that a dialect has no durable implementation.
	ErrUnsupportedDialect = errors.New("unsupported database dialect")
	// ErrSchemaOutdated indicates that the durable schema is absent or too old.
	ErrSchemaOutdated = errors.New("schema is absent or outdated")
	// ErrSchemaNewer indicates that the durable schema is newer than this runtime.
	ErrSchemaNewer = errors.New("schema is newer than runtime")
)

// ParseDialect validates a public dialect name.
func ParseDialect(name string) (Dialect, error) {
	switch name {
	case "sqlite":
		return dialectSQLite, nil
	case "postgres":
		return dialectPostgres, nil
	case "mysql":
		return dialectMySQL, nil
	default:
		return 0, ErrUnsupportedDialect
	}
}

// Verify checks schema compatibility without executing DDL.
func Verify(ctx context.Context, database *sql.DB, dialect Dialect) error {
	if dialect != dialectSQLite {
		return fmt.Errorf("%w: durable support is not implemented", ErrUnsupportedDialect)
	}

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

// Migrate applies all pending migrations for a dialect.
func Migrate(ctx context.Context, database *sql.DB, dialect Dialect) error {
	if dialect != dialectSQLite {
		return fmt.Errorf("%w: durable support is not implemented", ErrUnsupportedDialect)
	}

	if err := migrateWithRetry(ctx, database, dialect); err != nil {
		return err
	}

	if err := Verify(ctx, database, dialect); err != nil {
		return fmt.Errorf("verify migrated schema: %w", err)
	}

	return nil
}

func migrateWithRetry(ctx context.Context, database *sql.DB, dialect Dialect) error {
	const (
		attempts = 20
		delay    = 10 * time.Millisecond
	)

	for attempt := range attempts {
		provider, err := newProvider(database, dialect)
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

func newProvider(database *sql.DB, dialect Dialect) (*goose.Provider, error) {
	gooseDialect, migrations, err := providerConfig(dialect)
	if err != nil {
		return nil, err
	}

	provider, err := goose.NewProvider(
		gooseDialect,
		database,
		nil,
		goose.WithDisableGlobalRegistry(true),
		goose.WithGoMigrations(migrations...),
		goose.WithTableName(schemaVersionTable),
	)
	if err != nil {
		return nil, fmt.Errorf("configure goose: %w", err)
	}

	return provider, nil
}

func providerConfig(dialect Dialect) (goose.Dialect, []*goose.Migration, error) {
	switch dialect {
	case dialectSQLite:
		return goose.DialectSQLite3, sqliteMigrations(), nil
	case dialectPostgres, dialectMySQL:
		return goose.DialectCustom, nil, ErrUnsupportedDialect
	default:
		return goose.DialectCustom, nil, ErrUnsupportedDialect
	}
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
