package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

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
