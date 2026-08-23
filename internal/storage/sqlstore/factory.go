// Package sqlstore selects and bootstraps Cord's SQL storage backend.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/postgres"
	"github.com/omarluq/cord/internal/storage/sqlite"
)

const (
	postgresCapabilityProbe = "SELECT current_setting('server_version_num')::bigint"
	sqliteCapabilityProbe   = "SELECT sqlite_version()"
)

type dialect uint8

const (
	dialectSQLite dialect = iota
	dialectPostgres
)

type backend interface {
	storage.Backend
}

// Store is a dialect-independent SQL storage backend.
type Store struct {
	backend
}

var _ storage.Backend = (*Store)(nil)

// New detects the SQL dialect by querying backend capabilities, applies its
// migrations, and constructs the matching storage adapter.
func New(ctx context.Context, database *sql.DB) (*Store, error) {
	detectedDialect, err := detect(ctx, database)
	if err != nil {
		return nil, err
	}

	switch detectedDialect {
	case dialectSQLite:
		if err := sqlite.Migrate(ctx, database); err != nil {
			return nil, fmt.Errorf("migrate SQLite storage: %w", err)
		}

		store, err := sqlite.New(database)
		if err != nil {
			return nil, fmt.Errorf("create SQLite storage: %w", err)
		}

		return &Store{backend: store}, nil
	case dialectPostgres:
		if err := postgres.Migrate(ctx, database); err != nil {
			return nil, fmt.Errorf("migrate PostgreSQL storage: %w", err)
		}

		store, err := postgres.New(database)
		if err != nil {
			return nil, fmt.Errorf("create PostgreSQL storage: %w", err)
		}

		return &Store{backend: store}, nil
	default:
		return nil, fmt.Errorf("bootstrap SQL storage: unknown dialect %d", detectedDialect)
	}
}

func detect(ctx context.Context, database *sql.DB) (dialect, error) {
	var sqliteVersion string

	sqliteErr := database.QueryRowContext(ctx, sqliteCapabilityProbe).Scan(&sqliteVersion)
	if sqliteErr == nil {
		return dialectSQLite, nil
	}

	if ctx.Err() != nil {
		return 0, fmt.Errorf("detect SQL storage backend: %w", context.Cause(ctx))
	}

	var postgresVersion int64

	postgresErr := database.QueryRowContext(ctx, postgresCapabilityProbe).Scan(&postgresVersion)
	if postgresErr == nil {
		return dialectPostgres, nil
	}

	if ctx.Err() != nil {
		return 0, fmt.Errorf("detect SQL storage backend: %w", context.Cause(ctx))
	}

	return 0, fmt.Errorf(
		"detect SQL storage backend: %w",
		errors.Join(
			fmt.Errorf("SQLite capability probe: %w", sqliteErr),
			fmt.Errorf("PostgreSQL capability probe: %w", postgresErr),
		),
	)
}
