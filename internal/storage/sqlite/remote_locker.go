package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
)

const remoteMigrationLockPollInterval = 50 * time.Millisecond

type remoteMigrationLocker struct {
	owner string
}

func (locker *remoteMigrationLocker) SessionLock(ctx context.Context, connection *sql.Conn) error {
	owner, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("create remote migration lock owner: %w", err)
	}

	if err := createRemoteMigrationLockTable(ctx, connection); err != nil {
		return err
	}

	for {
		result, execErr := connection.ExecContext(ctx, `INSERT INTO cord_migration_lock (id, owner)
			VALUES (1, ?) ON CONFLICT(id) DO NOTHING`, owner.String())
		if execErr != nil {
			return fmt.Errorf("acquire remote migration lock: %w", execErr)
		}

		rows, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("inspect remote migration lock acquisition: %w", rowsErr)
		}

		if rows > 0 {
			locker.owner = owner.String()

			return nil
		}

		timer := time.NewTimer(remoteMigrationLockPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return fmt.Errorf("wait for remote sqlite migration lock: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func createRemoteMigrationLockTable(ctx context.Context, connection *sql.Conn) error {
	_, err := connection.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cord_migration_lock (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		owner TEXT NOT NULL
	)`)
	if err != nil {
		return fmt.Errorf("create remote migration lock table: %w", err)
	}

	return nil
}

func (locker *remoteMigrationLocker) SessionUnlock(ctx context.Context, connection *sql.Conn) error {
	if locker.owner == "" {
		return nil
	}

	if _, err := connection.ExecContext(
		ctx,
		"DELETE FROM cord_migration_lock WHERE id = 1 AND owner = ?",
		locker.owner,
	); err != nil {
		return fmt.Errorf("release remote migration lock: %w", err)
	}

	locker.owner = ""

	return nil
}
