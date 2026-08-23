package sqlstore

import (
	"context"
	"database/sql"
)

// Dialect is the SQL dialect identifier exposed to external package tests.
type Dialect = dialect

const (
	// DialectSQLite identifies SQLite in external package tests.
	DialectSQLite = dialectSQLite
	// DialectPostgres identifies PostgreSQL in external package tests.
	DialectPostgres = dialectPostgres
	// PostgresCapabilityProbe is the PostgreSQL detection query used by tests.
	PostgresCapabilityProbe = postgresCapabilityProbe
	// SQLiteCapabilityProbe is the SQLite detection query used by tests.
	SQLiteCapabilityProbe = sqliteCapabilityProbe
)

// Detect exposes dialect detection to external package tests.
func Detect(ctx context.Context, database *sql.DB) (Dialect, error) {
	return detect(ctx, database)
}
