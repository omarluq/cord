package remotelock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (locker *Locker) startRenewal(owner string, connection *sql.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	locker.owner = owner
	locker.cancel = cancel
	locker.renewalConnection = connection
	locker.renewalDone = done

	go func() {
		err := renew(ctx, connection, owner, locker.renewalInterval, nil)
		if err != nil && locker.cancelMigration != nil {
			locker.cancelMigration(err)
		}

		done <- err

		close(done)
	}()
}

func renew(
	ctx context.Context,
	connection *sql.Conn,
	owner string,
	interval time.Duration,
	onRenewal func(context.Context) bool,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		renewalCtx, cancel := context.WithTimeout(ctx, renewalTimeout)
		result, err := connection.ExecContext(renewalCtx, `UPDATE cord_migration_lock
			SET expires_at = unixepoch() + ? WHERE id = 1 AND owner = ?`,
			int64(leaseDuration/time.Second), owner)

		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf("renew remote migration lock: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect remote migration lock renewal: %w", err)
		}

		if rows == 0 {
			return ErrLockOwnershipLost
		}

		if onRenewal != nil && !onRenewal(ctx) {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func waitForRenewal(done <-chan error) error {
	timer := time.NewTimer(renewalTimeout)
	defer timer.Stop()

	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errors.New("wait for remote migration lock renewal to stop: timeout")
	}
}

func closeRenewalConnection(connection *sql.Conn) error {
	if err := connection.Close(); err != nil {
		return fmt.Errorf("close remote migration lock renewal connection: %w", err)
	}

	return nil
}
