package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
	"github.com/pressly/goose/v3"
)

const (
	initialVersion     = int64(1)
	requiredVersion    = int64(2)
	schemaVersionTable = "cord_schema_migrations"
	migrationLockKey   = int64(0x636f7264) // "cord"
)

func migrations() []*goose.Migration {
	return []*goose.Migration{
		goose.NewGoMigration(initialVersion, &goose.GoFunc{RunTx: migrateV1}, nil),
		goose.NewGoMigration(requiredVersion, &goose.GoFunc{RunTx: migrateV2}, nil),
	}
}

func migrateV1(ctx context.Context, transaction *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS cord_runs (
			id TEXT PRIMARY KEY,
			workflow_name TEXT NOT NULL,
			definition_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			input_payload BYTEA NOT NULL,
			output_payload BYTEA,
			terminal_node_id TEXT NOT NULL,
			error_payload BYTEA,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			completed_at TIMESTAMPTZ,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			retry_base_delay_ns BIGINT NOT NULL DEFAULT 500000000,
			retry_max_delay_ns BIGINT NOT NULL DEFAULT 30000000000,
			retry_policy_version INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE IF NOT EXISTS cord_nodes (
			run_id TEXT NOT NULL REFERENCES cord_runs(id) ON DELETE CASCADE,
			node_id TEXT NOT NULL,
			function_key TEXT NOT NULL,
			signature_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			remaining_deps INTEGER NOT NULL,
			attempt INTEGER NOT NULL,
			available_at TIMESTAMPTZ NOT NULL,
			lease_owner TEXT,
			lease_generation BIGINT NOT NULL,
			lease_expires_at TIMESTAMPTZ,
			output_payload BYTEA,
			error_payload BYTEA,
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			PRIMARY KEY (run_id, node_id)
		)`,
		`CREATE TABLE IF NOT EXISTS cord_edges (
			run_id TEXT NOT NULL,
			parent_node_id TEXT NOT NULL,
			child_node_id TEXT NOT NULL,
			parent_order INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (run_id, parent_node_id, child_node_id),
			FOREIGN KEY (run_id, parent_node_id)
				REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE,
			FOREIGN KEY (run_id, child_node_id)
				REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS cord_nodes_status_available_at_idx
			ON cord_nodes(status, available_at)`,
		`CREATE INDEX IF NOT EXISTS cord_nodes_function_status_available_at_idx
			ON cord_nodes(function_key, status, available_at)`,
		`CREATE INDEX IF NOT EXISTS cord_nodes_run_status_idx
			ON cord_nodes(run_id, status)`,
		`CREATE INDEX IF NOT EXISTS cord_nodes_lease_expires_at_idx
			ON cord_nodes(lease_expires_at)`,
	}

	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute initial postgres schema statement: %w", err)
		}
	}

	return nil
}

func migrateV2(ctx context.Context, transaction *sql.Tx) error {
	const statement = `CREATE INDEX IF NOT EXISTS cord_edges_run_child_parent_order_idx
		ON cord_edges(run_id, child_node_id, parent_order)`
	if _, err := transaction.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("create parent-output lookup index: %w", err)
	}

	return nil
}

// Migrate applies PostgreSQL schema migrations while holding a session advisory lock.
func Migrate(ctx context.Context, database *sql.DB) error {
	if database == nil {
		return errors.New("migrate postgres schema: database is nil")
	}

	// Goose resolves migration ordering before acquiring its configured locker. Reject a
	// newer schema first so that this compatibility check cannot be hidden by Goose's
	// out-of-order diagnostic. The locker repeats it after serialization.
	if err := runTransaction(ctx, database, "preflight postgres migrations", func(transaction *sql.Tx) error {
		return preflightMigration(ctx, transaction)
	}); err != nil {
		return err
	}

	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		database,
		nil,
		goose.WithDisableGlobalRegistry(true),
		goose.WithGoMigrations(migrations()...),
		goose.WithTableName(schemaVersionTable),
		goose.WithSessionLocker(sessionLocker{}),
	)
	if err != nil {
		return fmt.Errorf("configure postgres migration provider: %w", err)
	}

	if _, err = provider.Up(ctx); err != nil {
		return fmt.Errorf("apply postgres migrations: %w", err)
	}

	if err = Verify(ctx, database); err != nil {
		return fmt.Errorf("verify migrated postgres schema: %w", err)
	}

	return nil
}

type sessionLocker struct{}

func (sessionLocker) SessionLock(ctx context.Context, connection *sql.Conn) error {
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("lock postgres migrations: %w", err)
	}

	if err := preflightMigration(ctx, connection); err != nil {
		return errors.Join(err, unlockMigration(context.WithoutCancel(ctx), connection))
	}

	return nil
}

func (sessionLocker) SessionUnlock(ctx context.Context, connection *sql.Conn) error {
	return unlockMigration(ctx, connection)
}

func unlockMigration(ctx context.Context, connection *sql.Conn) error {
	var unlocked bool

	err := connection.QueryRowContext(
		ctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey,
	).Scan(&unlocked)
	if err != nil {
		return fmt.Errorf("unlock postgres migrations: %w", err)
	}

	if !unlocked {
		return errors.New("unlock postgres migrations: advisory lock was not held")
	}

	return nil
}

func preflightMigration(ctx context.Context, database rowQueryer) error {
	current, exists, err := schemaVersion(ctx, database)
	if err != nil {
		return err
	}

	if exists && current > requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", storage.ErrSchemaNewer, current, requiredVersion)
	}

	return nil
}

// Verify checks PostgreSQL schema compatibility without executing DDL.
func Verify(ctx context.Context, database *sql.DB) error {
	if database == nil {
		return errors.New("verify postgres schema: database is nil")
	}

	version, exists, err := schemaVersion(ctx, database)
	if err != nil {
		return err
	}

	if !exists || version < requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", storage.ErrSchemaOutdated, version, requiredVersion)
	}

	if version > requiredVersion {
		return fmt.Errorf("%w: current=%d required=%d", storage.ErrSchemaNewer, version, requiredVersion)
	}

	return verifySchemaStructure(ctx, database)
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func schemaVersion(ctx context.Context, database rowQueryer) (current int64, exists bool, err error) {
	const query = `SELECT to_regclass(format('%I.%I', current_schema(), 'cord_schema_migrations')) IS NOT NULL`
	if err = database.QueryRowContext(ctx, query).Scan(&exists); err != nil {
		return 0, false, fmt.Errorf("inspect postgres schema table: %w", err)
	}

	if !exists {
		return 0, false, nil
	}

	err = database.QueryRowContext(ctx, `SELECT COALESCE(MAX(version_id), 0)
		FROM cord_schema_migrations WHERE is_applied`).Scan(&current)
	if err != nil {
		return 0, true, fmt.Errorf("inspect postgres schema version: %w", err)
	}

	return current, true, nil
}
