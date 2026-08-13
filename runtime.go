package cord

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

// Dialect identifies a durable storage SQL dialect.
type Dialect string

const (
	// DialectSQLite identifies SQLite storage.
	DialectSQLite Dialect = "sqlite"
	// DialectPostgres identifies PostgreSQL storage.
	DialectPostgres Dialect = "postgres"
	// DialectMySQL identifies MySQL storage.
	DialectMySQL Dialect = "mysql"
)

// MigrationMode controls durable schema initialization.
type MigrationMode uint8

const (
	// MigrationVerifyOnly verifies the schema without changing it.
	MigrationVerifyOnly MigrationMode = iota
	// MigrationOnInitialization migrates the schema during durable runtime construction.
	MigrationOnInitialization
)

// DurableConfig configures a durable runtime.
type DurableConfig struct {
	DB            *sql.DB
	Dialect       Dialect
	MigrationMode MigrationMode
}

// Durable is a database-backed workflow runtime.
type Durable struct {
	*Cord
}

var (
	// ErrUnsupportedDialect indicates that the configured database dialect is unavailable.
	ErrUnsupportedDialect = errors.New("cord: unsupported database dialect")
	// ErrSchemaOutdated indicates that the Cord schema is absent or older than required.
	ErrSchemaOutdated = errors.New("cord: schema is absent or outdated")
	// ErrSchemaNewer indicates that the Cord schema is newer than this Cord version.
	ErrSchemaNewer = errors.New("cord: schema is newer than runtime")
	// ErrMigrationFailed indicates that Cord could not inspect or migrate its schema.
	ErrMigrationFailed = errors.New("cord: migration failed")
)

// NewDurable creates a durable runtime using a caller-owned database.
func NewDurable(config DurableConfig) (*Durable, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrMigrationFailed)
	}

	dialect, err := storage.ParseDialect(string(config.Dialect))
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedDialect, config.Dialect)
	}

	ctx := context.Background()

	switch config.MigrationMode {
	case MigrationVerifyOnly:
		err = storage.Verify(ctx, config.DB, dialect)
	case MigrationOnInitialization:
		err = storage.Migrate(ctx, config.DB, dialect)
	default:
		return nil, fmt.Errorf("%w: unknown migration mode %d", ErrMigrationFailed, config.MigrationMode)
	}

	if err != nil {
		return nil, publicMigrationError(err)
	}

	return &Durable{Cord: New()}, nil
}

// Close releases resources owned by the durable runtime. It never closes the caller-owned database.
func (d *Durable) Close() error {
	return nil
}

// Migrate applies all pending Cord migrations to a caller-owned database.
func Migrate(ctx context.Context, database *sql.DB, dialect Dialect) error {
	if database == nil {
		return fmt.Errorf("%w: database is nil", ErrMigrationFailed)
	}

	parsed, err := storage.ParseDialect(string(dialect))
	if err != nil {
		return fmt.Errorf("%w: %q", ErrUnsupportedDialect, dialect)
	}

	if err := storage.Migrate(ctx, database, parsed); err != nil {
		return publicMigrationError(err)
	}

	return nil
}

func publicMigrationError(err error) error {
	switch {
	case errors.Is(err, storage.ErrUnsupportedDialect):
		return fmt.Errorf("%w: %w", ErrUnsupportedDialect, err)
	case errors.Is(err, storage.ErrSchemaOutdated):
		return fmt.Errorf("%w: %w", ErrSchemaOutdated, err)
	case errors.Is(err, storage.ErrSchemaNewer):
		return fmt.Errorf("%w: %w", ErrSchemaNewer, err)
	default:
		return fmt.Errorf("%w: %w", ErrMigrationFailed, err)
	}
}
