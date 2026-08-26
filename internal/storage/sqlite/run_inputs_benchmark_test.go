package sqlite_test

import (
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	cordsqlite "github.com/omarluq/cord/internal/storage/sqlite"
	"testing"
	"time"
)

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
