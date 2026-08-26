package sqlite_test

import (
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	"testing"
)

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
