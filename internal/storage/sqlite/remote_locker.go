package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
)

const (
	remoteMigrationLockLease        = 30 * time.Second
	remoteMigrationLockPollInterval = 50 * time.Millisecond
	remoteMigrationLockRenewal      = 10 * time.Second
)

type remoteMigrationLocker struct {
	cancel      context.CancelFunc
	renewalDone <-chan error
	owner       string
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
		result, execErr := connection.ExecContext(
			ctx,
			`INSERT INTO cord_migration_lock (id, owner, expires_at)
			VALUES (1, ?, unixepoch() + ?) ON CONFLICT(id) DO UPDATE SET
				owner = excluded.owner,
				expires_at = excluded.expires_at
			WHERE cord_migration_lock.expires_at <= unixepoch()`,
			owner.String(),
			int64(remoteMigrationLockLease/time.Second),
		)
		if execErr != nil {
			return fmt.Errorf("acquire remote migration lock: %w", execErr)
		}

		rows, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("inspect remote migration lock acquisition: %w", rowsErr)
		}

		if rows > 0 {
			locker.startRenewal(connection, owner.String())

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
		owner TEXT NOT NULL,
		expires_at INTEGER NOT NULL
	)`)
	if err != nil {
		return fmt.Errorf("create remote migration lock table: %w", err)
	}

	return nil
}

func (locker *remoteMigrationLocker) startRenewal(connection *sql.Conn, owner string) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	locker.owner = owner
	locker.cancel = cancel
	locker.renewalDone = done

	go renewRemoteMigrationLock(ctx, connection, owner, done)
}

func renewRemoteMigrationLock(
	ctx context.Context,
	connection *sql.Conn,
	owner string,
	done chan<- error,
) {
	defer close(done)

	ticker := time.NewTicker(remoteMigrationLockRenewal)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			done <- nil

			return
		case <-ticker.C:
			result, err := connection.ExecContext(ctx, `UPDATE cord_migration_lock
				SET expires_at = unixepoch() + ? WHERE id = 1 AND owner = ?`,
				int64(remoteMigrationLockLease/time.Second), owner)
			if err != nil {
				continue
			}

			rows, err := result.RowsAffected()
			if err != nil {
				done <- fmt.Errorf("inspect remote migration lock renewal: %w", err)

				return
			}

			if rows == 0 {
				done <- errors.New("remote migration lock ownership lost")

				return
			}
		}
	}
}

func (locker *remoteMigrationLocker) SessionUnlock(ctx context.Context, connection *sql.Conn) error {
	if locker.owner == "" {
		return nil
	}

	locker.cancel()
	renewErr := <-locker.renewalDone

	result, err := connection.ExecContext(
		ctx,
		"DELETE FROM cord_migration_lock WHERE id = 1 AND owner = ?",
		locker.owner,
	)
	if err != nil {
		return fmt.Errorf("release remote migration lock: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect remote migration lock release: %w", err)
	}

	if rows == 0 {
		return errors.New("remote migration lock ownership lost")
	}

	locker.owner = ""
	locker.cancel = nil
	locker.renewalDone = nil

	return renewErr
}
