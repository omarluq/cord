// Package remotelock provides lease-based migration locking for remote SQLite databases.
package remotelock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
)

const (
	leaseDuration   = 30 * time.Second
	pollInterval    = 50 * time.Millisecond
	renewalInterval = 10 * time.Second
	renewalTimeout  = 10 * time.Second
)

// Option configures a Locker.
type Option func(*Locker)

// WithRenewalInterval configures how often the migration lease is renewed.
func WithRenewalInterval(interval time.Duration) Option {
	return func(locker *Locker) {
		locker.renewalInterval = interval
	}
}

// Locker implements Goose session locking with a renewable lease stored in SQLite.
type Locker struct {
	cancel          context.CancelFunc
	cancelMigration context.CancelCauseFunc
	renewalDone     <-chan error
	database        *sql.DB
	owner           string
	renewalInterval time.Duration
}

// New creates a Locker backed by database. If lease renewal fails,
// cancelMigration is called with the renewal error when it is non-nil.
func New(database *sql.DB, cancelMigration context.CancelCauseFunc, options ...Option) *Locker {
	locker := &Locker{
		cancel:          nil,
		cancelMigration: cancelMigration,
		database:        database,
		renewalDone:     nil,
		renewalInterval: renewalInterval,
		owner:           "",
	}
	for _, option := range options {
		option(locker)
	}

	return locker
}

// SessionLock acquires the remote SQLite migration lease.
func (locker *Locker) SessionLock(ctx context.Context, connection *sql.Conn) error {
	if err := validateRenewalInterval(locker.renewalInterval); err != nil {
		return err
	}

	owner, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("create remote migration lock owner: %w", err)
	}

	if err := createTable(ctx, connection); err != nil {
		return err
	}

	if err := acquire(ctx, connection, owner.String()); err != nil {
		return err
	}

	locker.startRenewal(owner.String())

	return nil
}

func acquire(ctx context.Context, connection *sql.Conn, owner string) error {
	for {
		result, err := connection.ExecContext(
			ctx,
			`INSERT INTO cord_migration_lock (id, owner, expires_at)
			VALUES (1, ?, unixepoch() + ?) ON CONFLICT(id) DO UPDATE SET
				owner = excluded.owner,
				expires_at = excluded.expires_at
			WHERE cord_migration_lock.expires_at <= unixepoch()`,
			owner,
			int64(leaseDuration/time.Second),
		)
		if err != nil {
			return fmt.Errorf("acquire remote migration lock: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect remote migration lock acquisition: %w", err)
		}

		if rows > 0 {
			return nil
		}

		timer := time.NewTimer(pollInterval)
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

func validateRenewalInterval(interval time.Duration) error {
	maximumInterval := leaseDuration - renewalTimeout - time.Second
	if interval <= 0 || interval >= maximumInterval {
		return fmt.Errorf(
			"invalid remote migration lock renewal interval %s: must be greater than zero and less than %s",
			interval,
			maximumInterval,
		)
	}

	return nil
}

func createTable(ctx context.Context, connection *sql.Conn) error {
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

func (locker *Locker) startRenewal(owner string) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	locker.owner = owner
	locker.cancel = cancel
	locker.renewalDone = done

	go func() {
		err := renew(ctx, locker.database, owner, locker.renewalInterval)
		if err != nil && locker.cancelMigration != nil {
			locker.cancelMigration(err)
		}

		done <- err

		close(done)
	}()
}

func renew(ctx context.Context, database *sql.DB, owner string, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		renewalCtx, cancel := context.WithTimeout(ctx, renewalTimeout)
		result, err := database.ExecContext(renewalCtx, `UPDATE cord_migration_lock
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
			return errors.New("remote migration lock ownership lost")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// SessionUnlock stops lease renewal and releases the remote SQLite migration lease.
func (locker *Locker) SessionUnlock(ctx context.Context, connection *sql.Conn) error {
	if locker.owner == "" {
		return nil
	}

	locker.cancel()

	var renewErr error
	select {
	case renewErr = <-locker.renewalDone:
	case <-time.After(renewalTimeout):
		renewErr = errors.New("wait for remote migration lock renewal to stop: timeout")
	}

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
