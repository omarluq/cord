package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/benchmarktest"
	postgresstore "github.com/omarluq/cord/internal/storage/postgres"
)

const (
	postgresCancellationStormSize  = 16
	postgresCancellationStormNodes = 100
)

// BenchmarkPostgresCancellationWideRun measures cancellation's node-update
// cost independently of run construction and fixture reset work.
func BenchmarkPostgresCancellationWideRun(b *testing.B) {
	for _, nodeCount := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			database, store := newPostgresCancellationBenchmarkStore(b)
			runID := storage.RunID(fmt.Sprintf("postgres-wide-%d", nodeCount))
			seedPostgresCancellationRows(b.Context(), b, database, runID, nodeCount)

			b.ReportAllocs()

			iterations := 0

			for b.Loop() {
				outcome, err := store.CancelRun(b.Context(), runID)
				if err != nil {
					b.Fatalf("cancel %d-node run: %v", nodeCount, err)
				}

				if outcome != storage.CancellationCanceled {
					b.Fatalf("cancellation outcome = %q", outcome)
				}

				iterations++

				b.StopTimer()
				resetPostgresCancellationBenchmark(b, database)
				b.StartTimer()
			}

			b.ReportMetric(float64(nodeCount+2), "rows/op")
			b.ReportMetric(float64(iterations*nodeCount)/b.Elapsed().Seconds(), "nodes/s")
		})
	}
}

// BenchmarkPostgresCancellationStorm measures concurrent cancellations while
// an unrelated run durably advances through claim, heartbeat, and completion.
func BenchmarkPostgresCancellationStorm(b *testing.B) {
	database, store := newPostgresCancellationBenchmarkStore(b)
	cancelRunIDs := make([]storage.RunID, postgresCancellationStormSize)

	for index := range postgresCancellationStormSize {
		runID := storage.RunID(fmt.Sprintf("postgres-storm-%02d", index))
		seedPostgresCancellationRows(
			b.Context(), b, database, runID, postgresCancellationStormNodes,
		)
		cancelRunIDs[index] = runID
	}

	seedPostgresCancellationRows(b.Context(), b, database, "postgres-storm-progress", 1)

	if _, err := database.ExecContext(b.Context(), `UPDATE cord_nodes
		SET function_key = $1, signature_hash = $2
		WHERE run_id = 'postgres-storm-progress'`,
		benchmarktest.FunctionKey, benchmarktest.Signature,
	); err != nil {
		b.Fatalf("configure progress node: %v", err)
	}

	b.ReportAllocs()

	iterations := 0

	for b.Loop() {
		start := make(chan struct{})
		results := make(chan error, postgresCancellationStormSize+1)

		var group sync.WaitGroup
		for _, runID := range cancelRunIDs {
			group.Go(func() {
				<-start

				outcome, err := store.CancelRun(b.Context(), runID)
				if err == nil && outcome != storage.CancellationCanceled {
					err = fmt.Errorf("cancel run %q: outcome %q", runID, outcome)
				}

				results <- err
			})
		}

		group.Go(func() {
			<-start

			results <- benchmarktest.Advance(b, store)
		})

		close(start)
		group.Wait()
		close(results)

		for err := range results {
			if err != nil {
				b.Fatal(err)
			}
		}

		iterations++

		b.StopTimer()
		resetPostgresCancellationBenchmark(b, database)
		b.StartTimer()
	}

	canceledNodes := iterations * postgresCancellationStormSize * postgresCancellationStormNodes
	b.ReportMetric(float64(canceledNodes)/b.Elapsed().Seconds(), "nodes/s")
	b.ReportMetric(postgresCancellationStormSize, "cancellations/op")
	b.ReportMetric(3, "durable-progress/op")
}

func newPostgresCancellationBenchmarkStore(b *testing.B) (*sql.DB, *postgresstore.Store) {
	b.Helper()

	database := openPostgres(b, postgresCancellationBenchmarkDSN(b))
	if err := postgresstore.Migrate(b.Context(), database); err != nil {
		b.Fatalf("migrate cancellation benchmark database: %v", err)
	}

	database.SetMaxOpenConns(postgresCancellationStormSize + 4)
	database.SetMaxIdleConns(postgresCancellationStormSize + 4)

	store, err := postgresstore.New(database)
	if err != nil {
		b.Fatalf("create cancellation benchmark store: %v", err)
	}

	return database, store
}

func postgresCancellationBenchmarkDSN(b *testing.B) string {
	b.Helper()

	baseDSN := os.Getenv("CORD_POSTGRES_DSN")
	if baseDSN == "" {
		b.Fatal("PostgreSQL test connection was not initialized")
	}

	schema := "cord_benchmark_" + strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()

	admin, err := sql.Open("pgx", baseDSN)
	if err != nil {
		b.Fatalf("open PostgreSQL benchmark administrator: %v", err)
	}

	if _, err = admin.ExecContext(b.Context(), "CREATE SCHEMA "+quotedSchema); err != nil {
		closeErr := admin.Close()
		b.Fatalf("create PostgreSQL benchmark schema: %v (close: %v)", err, closeErr)
	}

	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()

		if _, dropErr := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); dropErr != nil {
			b.Errorf("drop PostgreSQL benchmark schema: %v", dropErr)
		}

		if closeErr := admin.Close(); closeErr != nil {
			b.Errorf("close PostgreSQL benchmark administrator: %v", closeErr)
		}
	})

	parsed, err := url.Parse(baseDSN)
	if err == nil && parsed.Scheme != "" {
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()

		return parsed.String()
	}

	return baseDSN + " search_path=" + schema
}

func resetPostgresCancellationBenchmark(b *testing.B, database *sql.DB) {
	b.Helper()

	_, err := database.ExecContext(b.Context(), `UPDATE cord_runs
		SET status = 'running', started_at = NULL, completed_at = NULL,
			terminal_reason = NULL, terminal_runner_id = NULL`)
	if err != nil {
		b.Fatalf("reset cancellation benchmark runs: %v", err)
	}

	_, err = database.ExecContext(b.Context(), `UPDATE cord_nodes
		SET status = 'ready', attempt = 0, lease_owner = NULL, lease_generation = 0,
			lease_expires_at = NULL, output_payload = NULL, error_payload = NULL,
			started_at = NULL, completed_at = NULL, state_changed_at = NULL,
			last_started_at = NULL, last_runner_id = NULL, terminal_reason = NULL`)
	if err != nil {
		b.Fatalf("reset cancellation benchmark nodes: %v", err)
	}
}

// TestPostgresCancellationQueryPlanCapture logs a deterministic generic plan
// only when explicitly requested; benchmark CI does not depend on planner text.
func TestPostgresCancellationQueryPlanCapture(t *testing.T) {
	t.Parallel()

	if os.Getenv("CORD_CANCELLATION_QUERY_PLANS") == "" {
		t.Skip("set CORD_CANCELLATION_QUERY_PLANS=1 to log cancellation plans")
	}

	database := openPostgres(t, startPostgres(t))
	if err := postgresstore.Migrate(t.Context(), database); err != nil {
		t.Fatalf("migrate cancellation plan database: %v", err)
	}

	seedPostgresCancellationRows(t.Context(), t, database, "query-plan", 10000)

	if _, err := database.ExecContext(t.Context(), "ANALYZE cord_runs, cord_nodes"); err != nil {
		t.Fatalf("analyze cancellation plan fixture: %v", err)
	}

	const explainCancellation = `EXPLAIN (COSTS OFF, TIMING OFF, SUMMARY OFF, GENERIC_PLAN)
		UPDATE cord_nodes
		SET status = 'canceled', lease_owner = NULL, lease_expires_at = NULL,
			completed_at = $2, state_changed_at = $2, terminal_reason = 'canceled_by_request'
		WHERE run_id = $1 AND status IN ('pending', 'ready', 'running', 'retry_wait')`

	requirePostgresGenericPlan(t, database)

	rows, err := database.QueryContext(
		t.Context(), explainCancellation, storage.RunID("query-plan"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("explain cancellation update: %v", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			t.Errorf("close cancellation plan rows: %v", closeErr)
		}
	}()

	var plan strings.Builder

	for rows.Next() {
		var line string
		if scanErr := rows.Scan(&line); scanErr != nil {
			t.Fatalf("scan cancellation plan: %v", scanErr)
		}

		plan.WriteString(line)
		plan.WriteByte('\n')
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("iterate cancellation plan: %v", err)
	}

	t.Logf("PostgreSQL cancellation query plan:\n%s", plan.String())
}

func requirePostgresGenericPlan(t *testing.T, database *sql.DB) {
	t.Helper()

	var serverVersion int
	if err := database.QueryRowContext(t.Context(), "SHOW server_version_num").Scan(&serverVersion); err != nil {
		t.Fatalf("read PostgreSQL server version: %v", err)
	}

	if serverVersion < 160000 {
		t.Skipf("generic query-plan capture requires PostgreSQL 16 or newer; server version: %d", serverVersion)
	}
}

func seedPostgresCancellationRows(
	ctx context.Context,
	tb testing.TB,
	database *sql.DB,
	runID storage.RunID,
	nodeCount int,
) {
	tb.Helper()

	const runQuery = `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, terminal_node_id,
		created_at, updated_at, max_attempts, retry_base_delay_ns,
		retry_max_delay_ns, retry_policy_version
	) VALUES ($1, 'cancellation-plan', 'cancellation-v1', 'running',
		'input', 'node-00000', statement_timestamp(), clock_timestamp(),
		3, 1000000, 1000000000, 1)`
	if _, err := database.ExecContext(ctx, runQuery, runID); err != nil {
		tb.Fatalf("seed cancellation plan run: %v", err)
	}

	const nodeQuery = `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_generation, state_changed_at
	) SELECT $1, 'node-' || lpad(node::text, 5, '0'), 'benchmark.CancelTarget',
		'cancel-target-v1', 'ready', 0, 0, clock_timestamp(), 0, clock_timestamp()
	FROM generate_series(0, $2 - 1) AS node`
	if _, err := database.ExecContext(ctx, nodeQuery, runID, nodeCount); err != nil {
		tb.Fatalf("seed cancellation plan nodes: %v", err)
	}
}
