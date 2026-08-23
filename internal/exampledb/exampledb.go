// Package exampledb opens databases used by executable examples and tests.
package exampledb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	// Register the database/sql drivers used by examples.
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// DB opens an in-memory SQLite database suitable for tests and examples.
func DB() *sql.DB {
	database, err := OpenSQLite(context.Background())
	if err != nil {
		panic(err)
	}

	return database
}

// OpenSQLite opens an in-memory modernc SQLite database.
func OpenSQLite(context.Context) (*sql.DB, error) {
	database, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}

	database.SetMaxOpenConns(1)

	return database, nil
}

// OpenPostgres opens and health-checks a pgx database/sql pool using
// CORD_POSTGRES_DSN.
func OpenPostgres(ctx context.Context) (*sql.DB, error) {
	const poolSize = 10

	dsn := os.Getenv("CORD_POSTGRES_DSN")
	if dsn == "" {
		return nil, errors.New("CORD_POSTGRES_DSN is required")
	}

	database, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: %w", err)
	}

	// Leave capacity for application queries, Cord workers, and migration work.
	database.SetMaxOpenConns(poolSize)
	database.SetMaxIdleConns(poolSize)

	if err := database.PingContext(ctx); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			return nil, fmt.Errorf("ping PostgreSQL: %w; close database: %w", err, closeErr)
		}

		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}

	return database, nil
}
