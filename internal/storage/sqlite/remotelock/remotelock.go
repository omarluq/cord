// Package remotelock provides lease-based migration locking for remote SQLite databases.
package remotelock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/omarluq/cord/internal/backoff"
)

const (
	leaseDuration       = 30 * time.Second
	initialPollInterval = 50 * time.Millisecond
	maximumPollInterval = 2 * time.Second
	renewalInterval     = 10 * time.Second
	renewalTimeout      = 10 * time.Second
)

var (
	// ErrInsufficientPoolCapacity indicates that remote migration locking cannot
	// reserve the connection needed to renew its lease.
	ErrInsufficientPoolCapacity = errors.New("remote migration locking requires at least two database connections")
	// ErrLockOwnershipLost indicates that another migrator replaced the lease.
	ErrLockOwnershipLost = errors.New("remote migration lock ownership lost")
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
	cancel            context.CancelFunc
	cancelMigration   context.CancelCauseFunc
	database          *sql.DB
	renewalConnection *sql.Conn
	renewalDone       <-chan error
	owner             string
	renewalInterval   time.Duration
}

// New creates a Locker backed by database. If lease renewal fails,
// cancelMigration is called with the renewal error when it is non-nil.
func New(database *sql.DB, cancelMigration context.CancelCauseFunc, options ...Option) *Locker {
	locker := &Locker{
		cancel:            nil,
		cancelMigration:   cancelMigration,
		database:          database,
		owner:             "",
		renewalConnection: nil,
		renewalDone:       nil,
		renewalInterval:   renewalInterval,
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

	if locker.database.Stats().MaxOpenConnections == 1 {
		return ErrInsufficientPoolCapacity
	}

	renewalConnection, err := locker.database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("reserve remote migration lock renewal connection: %w", err)
	}

	owner, err := uuid.NewV4()
	if err != nil {
		return errors.Join(
			fmt.Errorf("create remote migration lock owner: %w", err),
			closeRenewalConnection(renewalConnection),
		)
	}

	if err := createTable(ctx, connection); err != nil {
		return errors.Join(err, closeRenewalConnection(renewalConnection))
	}

	if err := acquire(ctx, connection, owner.String()); err != nil {
		return errors.Join(err, closeRenewalConnection(renewalConnection))
	}

	locker.startRenewal(owner.String(), renewalConnection)

	return nil
}

func acquire(ctx context.Context, connection *sql.Conn, owner string) error {
	for attempt := 1; ; attempt++ {
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

		delay := backoff.FullJitter(initialPollInterval, maximumPollInterval, attempt)

		timer := time.NewTimer(delay)
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
