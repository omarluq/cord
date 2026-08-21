package sqlite_test

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	cordsqlite "github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLiteQueryPlans_TimestampPredicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		query             string
		wantIndexDetail   string
		wantTemporarySort bool
	}{
		{
			name: "production claim predicate",
			query: `SELECT run_id, node_id FROM cord_nodes
				WHERE status = 'ready' AND julianday(available_at) <= julianday('now')
				ORDER BY julianday(available_at), run_id, node_id LIMIT 1`,
			wantIndexDetail:   "cord_nodes_status_available_at_idx (status=?)",
			wantTemporarySort: true,
		},
		{
			name: "index-compatible comparison candidate",
			query: `SELECT run_id, node_id FROM cord_nodes
				WHERE status = 'ready' AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
				ORDER BY available_at, run_id, node_id LIMIT 1`,
			wantIndexDetail:   "cord_nodes_status_available_at_idx (status=? AND available_at<?)",
			wantTemporarySort: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			database, _ := newStore(t, true)
			details := explainQueryPlan(t, database, testCase.query)
			joined := strings.Join(details, "\n")
			assert.Contains(t, joined, testCase.wantIndexDetail)
			assert.Equal(t, testCase.wantTemporarySort, strings.Contains(joined, "USE TEMP B-TREE"), joined)
		})
	}
}

func explainQueryPlan(t *testing.T, database *sql.DB, query string) []string {
	t.Helper()

	rows, err := database.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query)

	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var details []string

	for rows.Next() {
		var (
			id, parent, unused int
			detail             string
		)
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}

	require.NoError(t, rows.Err())

	return details
}

func BenchmarkStore_CreateRun(b *testing.B) {
	for _, nodeCount := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			store := newBenchmarkStore(b)

			b.ReportAllocs()
			b.ResetTimer()

			for index := range b.N {
				b.StopTimer()

				plan := benchmarkLinearPlan(storage.RunID(fmt.Sprintf("run-%d", index)), nodeCount)

				b.StartTimer()

				if err := store.CreateRun(b.Context(), &plan); err != nil {
					b.Fatalf("create %d-node run: %v", nodeCount, err)
				}
			}
		})
	}
}

func BenchmarkStore_PollingAndMaintenance(b *testing.B) {
	benchmarks := []struct {
		run  func(*cordsqlite.Store) error
		name string
	}{
		{
			name: "empty claim",
			run: func(store *cordsqlite.Store) error {
				_, _, err := store.ClaimReadyNode(b.Context(), "benchmark-worker", time.Minute)
				if err != nil {
					return fmt.Errorf("claim ready node: %w", err)
				}

				return nil
			},
		},
		{
			name: "promote zero due",
			run: func(store *cordsqlite.Store) error {
				_, err := store.PromoteRetries(b.Context())
				if err != nil {
					return fmt.Errorf("promote retries: %w", err)
				}

				return nil
			},
		},
		{
			name: "recover zero expired",
			run: func(store *cordsqlite.Store) error {
				_, err := store.RecoverExpiredLeases(b.Context())
				if err != nil {
					return fmt.Errorf("recover expired leases: %w", err)
				}

				return nil
			},
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			store := newBenchmarkStore(b)
			b.ReportAllocs()

			for range b.N {
				if err := benchmark.run(store); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStore_RegisteredEmptyClaim(b *testing.B) {
	for _, registrationCount := range []int{1, 10, 1000} {
		b.Run(fmt.Sprintf("registrations=%d", registrationCount), func(b *testing.B) {
			store := newBenchmarkStore(b)

			registrations := make([]storage.FunctionRegistration, registrationCount)
			for index := range registrationCount {
				registrations[index] = storage.FunctionRegistration{
					Key: fmt.Sprintf("function-%04d", index), Signature: fmt.Sprintf("signature-%04d", index),
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, claimed, claimErr := store.ClaimReadyNodeForFunctions(
					b.Context(), "benchmark-worker", time.Minute, registrations,
				)
				if claimErr != nil {
					b.Fatal(claimErr)
				}

				if claimed {
					b.Fatal("unexpected claim from empty database")
				}
			}
		})
	}
}

func BenchmarkStore_ClaimAndTransitions(b *testing.B) {
	benchmarks := []struct {
		run          func(*testing.B, *cordsqlite.Store, *storage.Claim) error
		name         string
		includeClaim bool
	}{
		{name: "claim", includeClaim: true, run: noopBenchmarkTransition},
		{name: "complete", includeClaim: false, run: completeBenchmarkTransition},
		{name: "heartbeat", includeClaim: false, run: heartbeatBenchmarkTransition},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			benchmarkTransitions(b, benchmark.includeClaim, benchmark.run)
		})
	}
}

func benchmarkTransitions(
	b *testing.B,
	includeClaim bool,
	transition func(*testing.B, *cordsqlite.Store, *storage.Claim) error,
) {
	b.Helper()
	store := newBenchmarkStore(b)
	b.ReportAllocs()

	for index := range b.N {
		b.StopTimer()

		plan := benchmarkLinearPlan(storage.RunID(fmt.Sprintf("transition-%d", index)), 1)
		if err := store.CreateRun(b.Context(), &plan); err != nil {
			b.Fatal(err)
		}

		if includeClaim {
			b.StartTimer()
		}

		claim, claimed, err := store.ClaimReadyNode(b.Context(), "benchmark-worker", time.Minute)
		if err != nil || !claimed {
			b.Fatalf("claim benchmark node: claimed=%t err=%v", claimed, err)
		}

		if !includeClaim {
			b.StartTimer()
		}

		if err := transition(b, store, claim); err != nil {
			b.Fatal(err)
		}
	}
}

func noopBenchmarkTransition(b *testing.B, _ *cordsqlite.Store, _ *storage.Claim) error {
	b.Helper()

	return nil
}

func completeBenchmarkTransition(b *testing.B, store *cordsqlite.Store, claim *storage.Claim) error {
	b.Helper()

	accepted, err := store.CompleteNode(b.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`1`))
	if err != nil {
		return fmt.Errorf("complete node: %w", err)
	}

	if !accepted {
		return errors.New("completion rejected")
	}

	return nil
}

func heartbeatBenchmarkTransition(b *testing.B, store *cordsqlite.Store, claim *storage.Claim) error {
	b.Helper()

	accepted, _, err := store.HeartbeatNode(b.Context(), claim.RunID, claim.NodeID, claim.Lease, time.Minute)
	if err != nil {
		return fmt.Errorf("heartbeat node: %w", err)
	}

	if !accepted {
		return errors.New("heartbeat rejected")
	}

	return nil
}

func BenchmarkStore_LoadNodeInputs(b *testing.B) {
	b.Run("root", func(b *testing.B) { benchmarkLoadNodeInputs(b, 1) })
	b.Run("child", func(b *testing.B) { benchmarkLoadNodeInputs(b, 2) })
}

func benchmarkLoadNodeInputs(b *testing.B, nodeCount int) {
	b.Helper()
	store := newBenchmarkStore(b)

	plan := benchmarkLinearPlan("load-inputs", nodeCount)
	if err := store.CreateRun(b.Context(), &plan); err != nil {
		b.Fatal(err)
	}

	if nodeCount > 1 {
		completeBenchmarkRoot(b, store)
	}

	nodeID := plan.Nodes[nodeCount-1].ID

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := store.LoadNodeInputs(b.Context(), plan.Run.ID, nodeID); err != nil {
			b.Fatal(err)
		}
	}
}

func completeBenchmarkRoot(b *testing.B, store *cordsqlite.Store) {
	b.Helper()

	claim, claimed, err := store.ClaimReadyNode(b.Context(), "benchmark-worker", time.Minute)
	if err != nil || !claimed {
		b.Fatalf("claim root node: claimed=%t err=%v", claimed, err)
	}

	accepted, err := store.CompleteNode(
		b.Context(), claim.RunID, claim.NodeID, claim.Lease, storage.EncodedPayload(`1`),
	)
	if err != nil || !accepted {
		b.Fatalf("complete root node: accepted=%t err=%v", accepted, err)
	}
}

func BenchmarkStore_GetRunResult(b *testing.B) {
	store := newBenchmarkStore(b)

	plan := benchmarkLinearPlan("poll-result", 1)
	if err := store.CreateRun(b.Context(), &plan); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := store.GetRunResult(b.Context(), plan.Run.ID); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchmarkStore(b *testing.B) *cordsqlite.Store {
	b.Helper()

	dsn := "file:" + filepath.Join(b.TempDir(), "performance.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"

	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		closeErr := database.Close()
		if closeErr != nil {
			b.Errorf("close benchmark database: %v", closeErr)
		}
	})

	migrateErr := cordsqlite.Migrate(b.Context(), database)
	if migrateErr != nil {
		b.Fatal(migrateErr)
	}

	store, err := cordsqlite.New(database)
	if err != nil {
		b.Fatal(err)
	}

	return store
}

func benchmarkLinearPlan(runID storage.RunID, nodeCount int) storage.RunPlan {
	now := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	plan := validPlan(now, runID)
	plan.Run.TerminalNodeID = storage.NodeID(fmt.Sprintf("node-%04d", nodeCount-1))
	plan.Nodes = make([]storage.Node, nodeCount)
	plan.Edges = make([]storage.Edge, 0, nodeCount-1)

	for index := range nodeCount {
		nodeID := storage.NodeID(fmt.Sprintf("node-%04d", index))
		status := storage.NodePending
		remainingDependencies := 1

		if index == 0 {
			status = storage.NodeReady
			remainingDependencies = 0
		}

		plan.Nodes[index] = newNode(
			runID, nodeID, "benchmark.Step", "benchmark-signature", status, now, remainingDependencies,
		)
		if index > 0 {
			plan.Edges = append(plan.Edges, storage.Edge{
				RunID: runID, Parent: plan.Nodes[index-1].ID, Child: nodeID, ParentOrder: 0,
			})
		}
	}

	return plan
}
