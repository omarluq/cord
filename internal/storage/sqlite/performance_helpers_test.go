package sqlite_test

import (
	"database/sql"
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	cordsqlite "github.com/omarluq/cord/internal/storage/sqlite"
	"path/filepath"
	"testing"
	"time"
)

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
