// Package sqlite implements Cord's SQLite persistence adapter.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite/remotelock"
	"github.com/pressly/goose/v3"
)

const (
	schemaVersionTable = "cord_schema_migrations"
	requiredVersion    = int64(5)
)

// Verify checks SQLite schema compatibility without executing DDL.
func Verify(ctx context.Context, database *sql.DB) error {
	current, exists, err := schemaVersion(ctx, database)
	if err != nil {
		return fmt.Errorf("inspect sqlite schema: %w", err)
	}

	if !exists || current < requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", storage.ErrSchemaOutdated, current, requiredVersion)
	}

	if current > requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", storage.ErrSchemaNewer, current, requiredVersion)
	}

	if err := verifySchemaStructure(ctx, database); err != nil {
		return fmt.Errorf("inspect sqlite schema structure: %w", err)
	}

	return nil
}

// Migrate applies all pending SQLite migrations.
func Migrate(ctx context.Context, database *sql.DB) error {
	return retry(ctx, "wait for concurrent migration", time.Time{}, func(err error) bool {
		return errors.Is(err, storage.ErrSchemaOutdated) || IsBusy(err) || isMigrationBootstrapRace(err)
	}, func(operationCtx context.Context) error {
		if err := migrateWithRetry(operationCtx, database); err != nil {
			return err
		}

		if err := Verify(operationCtx, database); err != nil {
			return fmt.Errorf("verify migrated schema: %w", err)
		}

		return nil
	})
}

func isMigrationBootstrapRace(err error) bool {
	if err == nil {
		return false
	}

	message := err.Error()

	return strings.Contains(message, "cord_schema_migrations") &&
		(strings.Contains(message, "already exists") || strings.Contains(message, "no such table"))
}

func migrateWithRetry(ctx context.Context, database *sql.DB) error {
	migrationCtx, cancelMigration := context.WithCancelCause(ctx)
	defer cancelMigration(nil)

	provider, err := newProvider(database, cancelMigration)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}

	err = retryContention(migrationCtx, "wait to retry migration", func(operationCtx context.Context) error {
		_, upErr := provider.Up(operationCtx)

		if ctx.Err() != nil {
			return context.Cause(ctx)
		}

		if cause := context.Cause(migrationCtx); cause != nil {
			return fmt.Errorf("migration lock renewal failed: %w", cause)
		}

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

func newProvider(database *sql.DB, cancelMigration context.CancelCauseFunc) (*goose.Provider, error) {
	options := []goose.ProviderOption{
		goose.WithDisableGlobalRegistry(true),
		goose.WithGoMigrations(migrations()...),
		goose.WithTableName(schemaVersionTable),
	}
	if migrationPolicy(database.Driver()) == migrationLockRemote {
		options = append(options, goose.WithSessionLocker(remotelock.New(database, cancelMigration)))
	} else {
		options = append(options, goose.WithSessionLocker(sessionLocker{}))
	}

	provider, err := goose.NewProvider(goose.DialectSQLite3, database, nil, options...)
	if err != nil {
		return nil, fmt.Errorf("configure goose: %w", err)
	}

	return provider, nil
}

func schemaVersion(ctx context.Context, database *sql.DB) (current int64, exists bool, err error) {
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
