package sqlite_test

import (
	"database/sql"
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	"testing"
)

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
