package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
	"github.com/pressly/goose/v3"
)

const (
	initialVersion     = int64(1)
	parentOrderVersion = int64(2)
	idempotencyVersion = int64(3)
	requiredVersion    = int64(4)
	schemaVersionTable = "cord_schema_migrations"
	migrationLockKey   = int64(0x636f7264) // "cord"
)

// Migrate applies PostgreSQL schema migrations while holding a session advisory lock.
func Migrate(ctx context.Context, database *sql.DB) error {
	if database == nil {
		return errors.New("migrate postgres schema: database is nil")
	}

	// Goose resolves migration ordering before acquiring its configured locker. Reject a
	// newer schema first so that this compatibility check cannot be hidden by Goose's
	// out-of-order diagnostic. The locker repeats it after serialization.
	if err := runTransaction(ctx, database, "preflight postgres migrations", func(transaction *sql.Tx) error {
		return preflightMigration(ctx, transaction)
	}); err != nil {
		return err
	}

	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		database,
		nil,
		goose.WithDisableGlobalRegistry(true),
		goose.WithGoMigrations(migrations()...),
		goose.WithTableName(schemaVersionTable),
		goose.WithSessionLocker(sessionLocker{}),
	)
	if err != nil {
		return fmt.Errorf("configure postgres migration provider: %w", err)
	}

	if _, err = provider.Up(ctx); err != nil {
		return fmt.Errorf("apply postgres migrations: %w", err)
	}

	if err = Verify(ctx, database); err != nil {
		return fmt.Errorf("verify migrated postgres schema: %w", err)
	}

	return nil
}

type sessionLocker struct{}

func (sessionLocker) SessionLock(ctx context.Context, connection *sql.Conn) error {
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("lock postgres migrations: %w", err)
	}

	if err := preflightMigration(ctx, connection); err != nil {
		return errors.Join(err, unlockMigration(context.WithoutCancel(ctx), connection))
	}

	return nil
}

func (sessionLocker) SessionUnlock(ctx context.Context, connection *sql.Conn) error {
	return unlockMigration(ctx, connection)
}

func unlockMigration(ctx context.Context, connection *sql.Conn) error {
	var unlocked bool

	err := connection.QueryRowContext(
		ctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey,
	).Scan(&unlocked)
	if err != nil {
		return fmt.Errorf("unlock postgres migrations: %w", err)
	}

	if !unlocked {
		return errors.New("unlock postgres migrations: advisory lock was not held")
	}

	return nil
}

func preflightMigration(ctx context.Context, database rowQueryer) error {
	current, exists, err := schemaVersion(ctx, database)
	if err != nil {
		return err
	}

	if exists && current > requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", storage.ErrSchemaNewer, current, requiredVersion)
	}

	return nil
}

// Verify checks PostgreSQL schema compatibility without executing DDL.
func Verify(ctx context.Context, database *sql.DB) error {
	if database == nil {
		return errors.New("verify postgres schema: database is nil")
	}

	version, exists, err := schemaVersion(ctx, database)
	if err != nil {
		return err
	}

	if !exists || version < requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", storage.ErrSchemaOutdated, version, requiredVersion)
	}

	if version > requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", storage.ErrSchemaNewer, version, requiredVersion)
	}

	return verifySchemaStructure(ctx, database)
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func schemaVersion(ctx context.Context, database rowQueryer) (current int64, exists bool, err error) {
	const query = `SELECT to_regclass(format('%I.%I', current_schema(), 'cord_schema_migrations')) IS NOT NULL`
	if err = database.QueryRowContext(ctx, query).Scan(&exists); err != nil {
		return 0, false, fmt.Errorf("inspect postgres schema table: %w", err)
	}

	if !exists {
		return 0, false, nil
	}

	err = database.QueryRowContext(ctx, `SELECT COALESCE(MAX(version_id), 0)
		FROM cord_schema_migrations WHERE is_applied`).Scan(&current)
	if err != nil {
		return 0, true, fmt.Errorf("inspect postgres schema version: %w", err)
	}

	return current, true, nil
}
