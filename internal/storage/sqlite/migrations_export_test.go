package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// IsMigrationRetryableForTest exposes migration retry classification to external tests.
func IsMigrationRetryableForTest(err error) bool {
	return isMigrationRetryable(err)
}

// MigrateToVersionForTest applies SQLite migrations through the requested version.
func MigrateToVersionForTest(ctx context.Context, database *sql.DB, version int64) error {
	provider, err := newProvider(database, func(error) {})
	if err != nil {
		return err
	}

	_, err = provider.UpTo(ctx, version)
	if err != nil {
		return fmt.Errorf("migrate sqlite through version %d: %w", version, err)
	}

	return nil
}
