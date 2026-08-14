package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type sqliteSessionLocker struct{}

// SessionLock acquires an exclusive SQLite lock for Goose migrations.
func (sqliteSessionLocker) SessionLock(ctx context.Context, connection *sql.Conn) error {
	return retrySQLiteContention(ctx, "wait for sqlite migration lock", func() error {
		if err := initializeSQLiteLocking(ctx, connection); err != nil {
			return err
		}

		if _, err := connection.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
			return errors.Join(
				fmt.Errorf("acquire exclusive sqlite lock: %w", err),
				restoreSQLiteNormalLocking(context.WithoutCancel(ctx), connection),
			)
		}

		if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
			_, rollbackErr := connection.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")

			return errors.Join(
				fmt.Errorf("commit sqlite migration lock transaction: %w", err),
				wrapSQLiteRollbackError(rollbackErr),
				restoreSQLiteNormalLocking(context.WithoutCancel(ctx), connection),
			)
		}

		return nil
	})
}

// SessionUnlock releases the exclusive SQLite migration lock.
func (sqliteSessionLocker) SessionUnlock(ctx context.Context, connection *sql.Conn) error {
	return restoreSQLiteNormalLocking(ctx, connection)
}

func initializeSQLiteLocking(ctx context.Context, connection *sql.Conn) error {
	var schemaEntries int
	if err := connection.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema").Scan(&schemaEntries); err != nil {
		return fmt.Errorf("initialize normal sqlite locking: %w", err)
	}

	var mode string
	if err := connection.QueryRowContext(ctx, "PRAGMA locking_mode = EXCLUSIVE").Scan(&mode); err != nil {
		return fmt.Errorf("enable exclusive sqlite locking: %w", err)
	}

	return nil
}

func restoreSQLiteNormalLocking(ctx context.Context, connection *sql.Conn) error {
	var mode string

	modeErr := connection.QueryRowContext(ctx, "PRAGMA locking_mode = NORMAL").Scan(&mode)

	var schemaEntries int

	releaseErr := connection.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema").Scan(&schemaEntries)

	return errors.Join(
		wrapSQLiteLockCleanupError("restore normal sqlite locking", modeErr),
		wrapSQLiteLockCleanupError("release exclusive sqlite lock", releaseErr),
	)
}

func wrapSQLiteLockCleanupError(operation string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s: %w", operation, err)
}

func wrapSQLiteRollbackError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("rollback sqlite migration lock transaction: %w", err)
}
