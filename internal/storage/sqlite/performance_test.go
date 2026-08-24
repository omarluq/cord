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

// TestSQLiteQueryPlans_TimestampPredicates verifies timestamp predicates use the expected indexes.
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

// TestSQLiteQueryPlans_NodeInspectionPages verifies node inspection pages use keyset indexes.
func TestSQLiteQueryPlans_NodeInspectionPages(t *testing.T) {
	t.Parallel()

	database, _ := newStore(t, true)

	const selectReport = `SELECT n.node_id, n.function_key, n.status, n.attempt,
		n.available_at, n.started_at, n.last_started_at, n.state_changed_at, n.completed_at,
		n.lifecycle_version, n.terminal_reason, n.last_runner_id,
		n.lease_owner, n.lease_generation, n.lease_expires_at
	FROM cord_nodes AS n
	WHERE n.run_id = 'run' AND n.node_id > 'node-10'`

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "unfiltered keyset page",
			query: selectReport + ` ORDER BY n.node_id LIMIT 51`,
		},
		{
			name:  "state-filtered keyset page",
			query: selectReport + ` AND n.status = 'ready' ORDER BY n.node_id LIMIT 51`,
		},
		{
			name: "reason-filtered keyset page",
			query: selectReport + ` AND CASE
				WHEN n.lifecycle_version IS NULL AND n.status = 'completed' THEN 'succeeded'
				WHEN n.lifecycle_version IS NULL AND n.status = 'failed' THEN 'legacy_unknown'
				WHEN n.lifecycle_version IS NULL AND n.status = 'canceled' AND 'failed' = 'failed'
					THEN 'canceled_by_run_failure'
				WHEN n.lifecycle_version IS NULL AND n.status = 'canceled' THEN 'legacy_unknown'
				ELSE COALESCE(n.terminal_reason, '') END = 'succeeded'
				ORDER BY n.node_id LIMIT 51`,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			details := strings.Join(explainQueryPlan(t, database, testCase.query), "\n")
			assert.Contains(t, details, "sqlite_autoindex_cord_nodes_1 (run_id=? AND node_id>?)")
			assert.NotContains(t, details, "SCAN cord_nodes")
			assert.NotContains(t, details, "USE TEMP B-TREE")
		})
	}
}

// TestSQLiteQueryPlan_RunInspectionCounts verifies run inspection counts use run and status indexes.
func TestSQLiteQueryPlan_RunInspectionCounts(t *testing.T) {
	t.Parallel()

	database, _ := newStore(t, true)
	query := `SELECT
		r.id, r.workflow_name, r.status, r.created_at, r.updated_at,
		r.started_at, r.completed_at, r.lifecycle_version,
		r.terminal_reason, r.terminal_runner_id,
		COUNT(n.node_id),
		COALESCE(SUM(CASE WHEN n.status IN (
			'pending', 'ready', 'running', 'retry_wait', 'completed', 'failed', 'canceled'
		) THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = 'pending' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = 'ready' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = 'running' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = 'retry_wait' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = 'completed' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = 'failed' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = 'canceled' THEN 1 ELSE 0 END), 0)
	FROM cord_runs AS r
	LEFT JOIN cord_nodes AS n ON n.run_id = r.id
	WHERE r.id = 'run'
	GROUP BY r.id`

	details := strings.Join(explainQueryPlan(t, database, query), "\n")
	assert.Contains(t, details, "sqlite_autoindex_cord_runs_1 (id=?)")
	assert.Contains(t, details, "cord_nodes_run_status_idx (run_id=?)")
	assert.NotContains(t, details, "SCAN cord_nodes")
	assert.NotContains(t, details, "USE TEMP B-TREE")
}

// TestSQLiteQueryPlan_OrderedParentInputs verifies ordered parent inputs avoid a temporary sort.
func TestSQLiteQueryPlan_OrderedParentInputs(t *testing.T) {
	t.Parallel()

	database, _ := newStore(t, true)
	query := `SELECT p.output_payload FROM cord_edges AS e
		JOIN cord_nodes AS p ON p.run_id = e.run_id AND p.node_id = e.parent_node_id
		WHERE e.run_id = 'run' AND e.child_node_id = 'child' ORDER BY e.parent_order`

	_, err := database.ExecContext(t.Context(), "DROP INDEX cord_edges_run_child_parent_order_idx")
	require.NoError(t, err)

	before := strings.Join(explainQueryPlan(t, database, query), "\n")
	assert.Contains(t, before, "sqlite_autoindex_cord_edges_1 (run_id=?)")
	assert.Contains(t, before, "USE TEMP B-TREE FOR ORDER BY")

	_, err = database.ExecContext(t.Context(), `CREATE INDEX cord_edges_run_child_parent_order_idx
		ON cord_edges(run_id, child_node_id, parent_order)`)
	require.NoError(t, err)

	after := strings.Join(explainQueryPlan(t, database, query), "\n")
	assert.Contains(t, after,
		"cord_edges_run_child_parent_order_idx (run_id=? AND child_node_id=?)")
	assert.NotContains(t, after, "USE TEMP B-TREE FOR ORDER BY")
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

// BenchmarkStore_CreateRun measures run creation across plan sizes and index configurations.
func BenchmarkStore_CreateRun(b *testing.B) {
	for _, nodeCount := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			benchmarkCreateRun(b, nodeCount, true)
		})
	}

	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("index-cost/nodes=1000/indexed=%t", indexed), func(b *testing.B) {
			benchmarkCreateRun(b, 1000, indexed)
		})
	}
}

func benchmarkCreateRun(b *testing.B, nodeCount int, indexed bool) {
	b.Helper()

	store, database := newBenchmarkStoreWithDatabase(b)
	if !indexed {
		if _, err := database.ExecContext(b.Context(), "DROP INDEX cord_edges_run_child_parent_order_idx"); err != nil {
			b.Fatal(err)
		}
	}

	baselineBytes := benchmarkDatabaseBytes(b, database)

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

	b.StopTimer()

	growthBytes := benchmarkDatabaseBytes(b, database) - baselineBytes
	b.ReportMetric(float64(growthBytes)/float64(b.N), "database-bytes/run")
}

func benchmarkDatabaseBytes(b *testing.B, database *sql.DB) int64 {
	b.Helper()

	var pageCount, freePages, pageSize int64
	for pragma, target := range map[string]*int64{
		"PRAGMA page_count":     &pageCount,
		"PRAGMA freelist_count": &freePages,
		"PRAGMA page_size":      &pageSize,
	} {
		if err := database.QueryRowContext(b.Context(), pragma).Scan(target); err != nil {
			b.Fatal(err)
		}
	}

	return (pageCount - freePages) * pageSize
}

// BenchmarkStore_PollingAndMaintenance measures idle polling and maintenance operations.
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

// BenchmarkStore_RegisteredEmptyClaim measures empty claims across registration counts.
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

// BenchmarkStore_ClaimAndTransitions measures claiming and common node transitions.
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

// BenchmarkStore_LoadNodeInputs measures loading root and ordered child inputs.
func BenchmarkStore_LoadNodeInputs(b *testing.B) {
	b.Run("root", func(b *testing.B) { benchmarkLoadNodeInputs(b, 1) })
	b.Run("child", func(b *testing.B) { benchmarkLoadNodeInputs(b, 2) })

	for _, edgeCount := range []int{100, 1000} {
		for _, indexed := range []bool{false, true} {
			name := fmt.Sprintf("ordered-child/edges=%d/indexed=%t", edgeCount, indexed)
			b.Run(name, func(b *testing.B) { benchmarkOrderedChildInputs(b, edgeCount, indexed) })
		}
	}
}

func benchmarkOrderedChildInputs(b *testing.B, edgeCount int, indexed bool) {
	b.Helper()

	store, database := newBenchmarkStoreWithDatabase(b)
	if !indexed {
		if _, err := database.ExecContext(b.Context(), "DROP INDEX cord_edges_run_child_parent_order_idx"); err != nil {
			b.Fatal(err)
		}
	}

	plan := benchmarkFanInPlan("ordered-child-inputs", edgeCount)
	if err := store.CreateRun(b.Context(), &plan); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := store.LoadNodeInputs(b.Context(), plan.Run.ID, plan.Run.TerminalNodeID); err != nil {
			b.Fatal(err)
		}
	}
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

// BenchmarkStore_GetRunResult measures polling for a run result.
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

// BenchmarkStore_InspectRun measures run inspection across plan sizes.
func BenchmarkStore_InspectRun(b *testing.B) {
	for _, nodeCount := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			store := newBenchmarkStore(b)

			plan := benchmarkLinearPlan("inspect-run", nodeCount)
			if err := store.CreateRun(b.Context(), &plan); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if _, err := store.InspectRun(b.Context(), plan.Run.ID); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStore_ListRunNodes measures paginated node inspection.
func BenchmarkStore_ListRunNodes(b *testing.B) {
	for _, pageSize := range []int{1, 50, 200} {
		b.Run(fmt.Sprintf("first-page/size=%d", pageSize), func(b *testing.B) {
			benchmarkListRunNodes(b, storage.NodeQuery{
				State: nil, Reason: nil, ContinuationToken: "", PageSize: pageSize,
			})
		})
	}

	pending := storage.NodePending
	middleCursor := "node-0499"

	b.Run("middle-page/unfiltered", func(b *testing.B) {
		benchmarkListRunNodes(b, storage.NodeQuery{
			State: nil, Reason: nil, ContinuationToken: middleCursor, PageSize: 50,
		})
	})
	b.Run("middle-page/state-filtered", func(b *testing.B) {
		benchmarkListRunNodes(b, storage.NodeQuery{
			State: &pending, Reason: nil, ContinuationToken: middleCursor, PageSize: 50,
		})
	})
}

func benchmarkListRunNodes(b *testing.B, query storage.NodeQuery) {
	b.Helper()

	store := newBenchmarkStore(b)

	plan := benchmarkLinearPlan("list-run-nodes", 1000)
	if err := store.CreateRun(b.Context(), &plan); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		page, err := store.ListRunNodes(b.Context(), plan.Run.ID, query)
		if err != nil {
			b.Fatal(err)
		}

		if len(page.Nodes) == 0 {
			b.Fatal("node page is unexpectedly empty")
		}
	}
}

func newBenchmarkStore(b *testing.B) *cordsqlite.Store {
	b.Helper()

	store, _ := newBenchmarkStoreWithDatabase(b)

	return store
}

func newBenchmarkStoreWithDatabase(b *testing.B) (*cordsqlite.Store, *sql.DB) {
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

	return store, database
}

func benchmarkFanInPlan(runID storage.RunID, parentCount int) storage.RunPlan {
	now := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	plan := validPlan(now, runID)
	terminalID := storage.NodeID("terminal")
	plan.Run.TerminalNodeID = terminalID
	plan.Nodes = make([]storage.Node, 0, parentCount+1)
	plan.Edges = make([]storage.Edge, 0, parentCount)

	for index := range parentCount {
		parentID := storage.NodeID(fmt.Sprintf("parent-%04d", index))
		parent := newNode(runID, parentID, "benchmark.Parent", "benchmark-signature", storage.NodeReady, now, 0)
		plan.Nodes = append(plan.Nodes, parent)
		plan.Edges = append(plan.Edges, storage.Edge{
			RunID: runID, Parent: parentID, Child: terminalID, ParentOrder: index,
		})
	}

	plan.Nodes = append(plan.Nodes, newNode(
		runID, terminalID, "benchmark.Child", "benchmark-signature", storage.NodePending, now, parentCount,
	))

	return plan
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
