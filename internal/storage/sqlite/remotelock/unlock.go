package remotelock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// SessionUnlock stops lease renewal and releases the remote SQLite migration lease.
func (locker *Locker) SessionUnlock(ctx context.Context, connection *sql.Conn) error {
	if locker.owner == "" {
		return nil
	}

	owner := locker.owner
	cancel := locker.cancel
	renewalDone := locker.renewalDone
	renewalConnection := locker.renewalConnection

	locker.owner = ""
	locker.cancel = nil
	locker.renewalDone = nil
	locker.renewalConnection = nil

	cancel()

	renewErr := waitForRenewal(renewalDone)

	result, releaseErr := connection.ExecContext(
		ctx,
		"DELETE FROM cord_migration_lock WHERE id = 1 AND owner = ?",
		owner,
	)
	if releaseErr != nil {
		releaseErr = fmt.Errorf("release remote migration lock: %w", releaseErr)
	} else {
		rows, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			releaseErr = fmt.Errorf("inspect remote migration lock release: %w", rowsErr)
		} else if rows == 0 {
			releaseErr = ErrLockOwnershipLost
		}
	}

	return errors.Join(renewErr, releaseErr, closeRenewalConnection(renewalConnection))
}
