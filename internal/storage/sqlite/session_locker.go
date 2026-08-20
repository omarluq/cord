package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type sessionLocker struct{}

// SessionLock acquires an exclusive SQLite lock for Goose migrations.
func (sessionLocker) SessionLock(ctx context.Context, connection *sql.Conn) error {
	return retryContention(ctx, "wait for sqlite migration lock", func(operationCtx context.Context) error {
		if err := initializeLocking(operationCtx, connection); err != nil {
			return err
		}

		if _, err := connection.ExecContext(operationCtx, "BEGIN EXCLUSIVE"); err != nil {
			return errors.Join(
				fmt.Errorf("acquire exclusive sqlite lock: %w", err),
				restoreNormalLocking(context.WithoutCancel(ctx), connection),
			)
		}

		if _, err := connection.ExecContext(operationCtx, "COMMIT"); err != nil {
			_, rollbackErr := connection.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")

			return errors.Join(
				fmt.Errorf("commit sqlite migration lock transaction: %w", err),
				wrapRollbackError(rollbackErr),
				restoreNormalLocking(context.WithoutCancel(ctx), connection),
			)
		}

		return nil
	})
}

// SessionUnlock releases the exclusive SQLite migration lock.
func (sessionLocker) SessionUnlock(ctx context.Context, connection *sql.Conn) error {
	return restoreNormalLocking(ctx, connection)
}

func initializeLocking(ctx context.Context, connection *sql.Conn) error {
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

func restoreNormalLocking(ctx context.Context, connection *sql.Conn) error {
	var mode string

	modeErr := connection.QueryRowContext(ctx, "PRAGMA locking_mode = NORMAL").Scan(&mode)

	var schemaEntries int

	releaseErr := connection.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema").Scan(&schemaEntries)

	return errors.Join(
		wrapLockCleanupError("restore normal sqlite locking", modeErr),
		wrapLockCleanupError("release exclusive sqlite lock", releaseErr),
	)
}

func wrapLockCleanupError(operation string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s: %w", operation, err)
}

func wrapRollbackError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("rollback sqlite migration lock transaction: %w", err)
}
