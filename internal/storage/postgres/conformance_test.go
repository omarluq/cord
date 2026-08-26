package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/conformance"
	"github.com/omarluq/cord/internal/storage/postgres"
)

func postgresHarness(dsn string) conformance.Harness {
	openFixture := func(tb testing.TB, _ string) *sql.DB {
		tb.Helper()

		return openPostgres(tb, dsn)
	}

	return conformance.Harness{
		Open:    openFixture,
		Migrate: postgres.Migrate,
		NewBackend: func(database *sql.DB) (storage.Backend, error) {
			return postgres.New(database)
		},
		ExpireLease: func(
			ctx context.Context,
			database *sql.DB,
			runID storage.RunID,
			nodeID storage.NodeID,
		) error {
			const query = `UPDATE cord_nodes SET lease_expires_at = TIMESTAMPTZ '2000-01-01 00:00:00+00'
				WHERE run_id = $1 AND node_id = $2`
			if _, err := database.ExecContext(ctx, query, runID, nodeID); err != nil {
				return fmt.Errorf("expire PostgreSQL lease: %w", err)
			}

			return nil
		},
		DeleteRun: func(ctx context.Context, database *sql.DB, runID storage.RunID) error {
			if _, err := database.ExecContext(ctx, "DELETE FROM cord_runs WHERE id = $1", runID); err != nil {
				return fmt.Errorf("delete PostgreSQL run: %w", err)
			}

			return nil
		},
		CountRunRows: func(
			ctx context.Context,
			database *sql.DB,
			runID storage.RunID,
		) (int, int, error) {
			var nodes, edges int

			if err := database.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM cord_nodes WHERE run_id = $1", runID).Scan(&nodes); err != nil {
				return 0, 0, fmt.Errorf("count PostgreSQL node rows: %w", err)
			}

			if err := database.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM cord_edges WHERE run_id = $1", runID).Scan(&edges); err != nil {
				return 0, 0, fmt.Errorf("count PostgreSQL edge rows: %w", err)
			}

			return nodes, edges, nil
		},
		LoadNodeStates: conformance.NewNodeStateLoader(
			"PostgreSQL",
			`SELECT node_id, status, error_payload,
				COALESCE(lease_owner, ''), lease_generation, lease_expires_at IS NOT NULL, attempt
				FROM cord_nodes WHERE run_id = $1`,
		),
	}
}

// TestPostgresConformance runs the shared storage contract against PostgreSQL.
func TestPostgresConformance(t *testing.T) {
	t.Parallel()

	conformance.Run(t, postgresHarness(startPostgres(t)))
}
