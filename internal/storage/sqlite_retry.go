package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/backoff"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const sqliteRetryAttempts = 20

func retrySQLiteContention(ctx context.Context, operation string, operationFunc func() error) error {
	const (
		baseDelay = 10 * time.Millisecond
		maxDelay  = 100 * time.Millisecond
	)

	for attempt := 1; attempt <= sqliteRetryAttempts; attempt++ {
		err := operationFunc()
		if err == nil || !isSQLiteContention(err) || attempt == sqliteRetryAttempts {
			return err
		}

		delay := backoff.FullJitter(baseDelay, maxDelay, attempt)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return fmt.Errorf("%s: %w", operation, ctx.Err())
		case <-timer.C:
		}
	}

	return nil
}

func isSQLiteContention(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}

	switch sqliteErr.Code() {
	case sqlite3.SQLITE_BUSY,
		sqlite3.SQLITE_BUSY_RECOVERY,
		sqlite3.SQLITE_BUSY_SNAPSHOT,
		sqlite3.SQLITE_BUSY_TIMEOUT:
		return true
	default:
		return false
	}
}
