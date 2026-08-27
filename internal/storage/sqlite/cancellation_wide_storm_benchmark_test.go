package sqlite_test

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	cordsqlite "github.com/omarluq/cord/internal/storage/sqlite"
)

const (
	sqliteCancellationStormSize  = 16
	sqliteCancellationStormNodes = 100
)

// BenchmarkSQLiteCancellationWideRun measures cancellation's node-update cost
// independently of run construction and fixture reset work.
func BenchmarkSQLiteCancellationWideRun(b *testing.B) {
	for _, nodeCount := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			store, database := newBenchmarkStoreWithDatabase(b)
			runID := storage.RunID(fmt.Sprintf("sqlite-wide-%d", nodeCount))

			plan := benchmarkFanInPlan(runID, nodeCount-1)
			if err := store.CreateRun(b.Context(), &plan); err != nil {
				b.Fatalf("seed cancellation run: %v", err)
			}

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
				resetSQLiteCancellationBenchmark(b, database)
				b.StartTimer()
			}

			b.ReportMetric(float64(nodeCount+2), "rows/op")
			b.ReportMetric(float64(iterations*nodeCount)/b.Elapsed().Seconds(), "nodes/s")
		})
	}
}

// BenchmarkSQLiteCancellationStorm measures concurrent cancellations while an
// unrelated run durably advances through claim, heartbeat, and completion.
func BenchmarkSQLiteCancellationStorm(b *testing.B) {
	store, database := newBenchmarkStoreWithDatabase(b)
	cancelRunIDs := make([]storage.RunID, sqliteCancellationStormSize)

	for index := range sqliteCancellationStormSize {
		runID := storage.RunID(fmt.Sprintf("sqlite-storm-%02d", index))

		plan := benchmarkFanInPlan(runID, sqliteCancellationStormNodes-1)
		if err := store.CreateRun(b.Context(), &plan); err != nil {
			b.Fatalf("seed storm run %q: %v", runID, err)
		}

		cancelRunIDs[index] = runID
	}

	progressPlan := benchmarkLinearPlan("sqlite-storm-progress", 1)
	progressPlan.Nodes[0].FunctionKey = "benchmark.CancellationProgress"

	progressPlan.Nodes[0].SignatureHash = "cancellation-progress-v1"
	if err := store.CreateRun(b.Context(), &progressPlan); err != nil {
		b.Fatalf("seed progress run: %v", err)
	}

	b.ReportAllocs()

	iterations := 0

	for b.Loop() {
		if err := runSQLiteCancellationStormIteration(b, store, cancelRunIDs); err != nil {
			b.Fatal(err)
		}

		iterations++

		b.StopTimer()
		resetSQLiteCancellationBenchmark(b, database)
		b.StartTimer()
	}

	canceledNodes := iterations * sqliteCancellationStormSize * sqliteCancellationStormNodes
	b.ReportMetric(float64(canceledNodes)/b.Elapsed().Seconds(), "nodes/s")
	b.ReportMetric(sqliteCancellationStormSize, "cancellations/op")
	b.ReportMetric(3, "durable-progress/op")
}

func runSQLiteCancellationStormIteration(
	b *testing.B,
	store *cordsqlite.Store,
	runIDs []storage.RunID,
) error {
	b.Helper()

	start := make(chan struct{})
	results := make(chan error, len(runIDs)+1)

	var group sync.WaitGroup
	for _, runID := range runIDs {
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

		results <- advanceSQLiteCancellationProgress(b, store)
	})

	close(start)
	group.Wait()
	close(results)

	var errs []error

	for err := range results {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func advanceSQLiteCancellationProgress(b *testing.B, store *cordsqlite.Store) error {
	b.Helper()

	registrations := []storage.FunctionRegistration{{
		Key: "benchmark.CancellationProgress", Signature: "cancellation-progress-v1",
	}}

	claim, claimed, err := store.ClaimReadyNodeForFunctions(
		b.Context(), "sqlite-progress-worker", time.Minute, registrations,
	)
	if err != nil {
		return fmt.Errorf("claim unrelated progress node: %w", err)
	}

	if !claimed {
		return errors.New("claim unrelated progress node: no claim")
	}

	accepted, _, err := store.HeartbeatNode(
		b.Context(), claim.RunID, claim.NodeID, claim.Lease, time.Minute,
	)
	if err != nil {
		return fmt.Errorf("heartbeat unrelated progress node: %w", err)
	}

	if !accepted {
		return errors.New("heartbeat unrelated progress node: rejected")
	}

	accepted, err = store.CompleteNode(
		b.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"done"`),
	)
	if err != nil {
		return fmt.Errorf("complete unrelated progress node: %w", err)
	}

	if !accepted {
		return errors.New("complete unrelated progress node: rejected")
	}

	return nil
}

func resetSQLiteCancellationBenchmark(b *testing.B, database *sql.DB) {
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

// TestSQLiteCancellationQueryPlanCapture logs the cancellation update plan
// only when explicitly requested; benchmark CI does not depend on planner text.
func TestSQLiteCancellationQueryPlanCapture(t *testing.T) {
	t.Parallel()

	if os.Getenv("CORD_CANCELLATION_QUERY_PLANS") == "" {
		t.Skip("set CORD_CANCELLATION_QUERY_PLANS=1 to log cancellation plans")
	}

	database, _ := newStore(t, true)
	query := `UPDATE cord_nodes
		SET status = 'canceled', lease_owner = NULL, lease_expires_at = NULL,
			completed_at = '2025-01-01T00:00:00.000Z',
			state_changed_at = '2025-01-01T00:00:00.000Z',
			terminal_reason = 'canceled_by_request'
		WHERE run_id = 'query-plan' AND status IN ('pending', 'ready', 'running', 'retry_wait')`

	t.Logf("SQLite cancellation query plan:\n%s", strings.Join(explainQueryPlan(t, database, query), "\n"))
}
