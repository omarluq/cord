package cord_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omarluq/cord"
)

const benchmarkLinearNodes = 8

func benchmarkStep(_ context.Context, value int) (int, error) {
	return value + 1, nil
}

func benchmarkJoin(_ context.Context, left, right int) (int, error) {
	return left + right, nil
}

func BenchmarkScheduler_Throughput(b *testing.B) {
	benchmarks := []struct {
		build func(*cord.Cord) (cord.Workflow[int, int], int)
		name  string
	}{
		{name: "linear", build: buildLinearBenchmark},
		{name: "branching", build: buildBranchingBenchmark},
	}

	for _, benchmark := range benchmarks {
		for _, concurrency := range []int{1, 4, 8} {
			b.Run(fmt.Sprintf("%s/concurrency=%d", benchmark.name, concurrency), func(b *testing.B) {
				runtime := newBenchmarkRuntime(b, concurrency)
				workflow, nodesPerRun := benchmark.build(runtime)

				benchmarkWorkflowThroughput(b, workflow, concurrency, nodesPerRun)
			})
		}
	}
}

func buildLinearBenchmark(runtime *cord.Cord) (workflow cord.Workflow[int, int], nodesPerRun int) {
	workflow = runtime.From("benchmark-linear", benchmarkStep)
	for range benchmarkLinearNodes - 1 {
		workflow = workflow.Then(benchmarkStep)
	}

	return workflow, benchmarkLinearNodes
}

func buildBranchingBenchmark(runtime *cord.Cord) (workflow cord.Workflow[int, int], nodesPerRun int) {
	root := runtime.From("benchmark-branching", benchmarkStep)
	workflow = cord.Join(root.Then(benchmarkStep), root.Then(benchmarkStep)).Then(benchmarkJoin)

	return workflow, 4
}

func newBenchmarkRuntime(b *testing.B, concurrency int) *cord.Cord {
	b.Helper()

	dsn := "file:" + filepath.Join(b.TempDir(), "cord.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"

	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatalf("open benchmark database: %v", err)
	}

	runtime, err := cord.New(b.Context(), database, cord.Options{
		Concurrency:  concurrency,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		if closeErr := database.Close(); closeErr != nil {
			b.Errorf("close benchmark database after runtime creation failure: %v", closeErr)
		}

		b.Fatalf("create benchmark runtime: %v", err)
	}

	b.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			b.Errorf("close benchmark runtime: %v", err)
		}

		if err := database.Close(); err != nil {
			b.Errorf("close benchmark database: %v", err)
		}
	})

	return runtime
}

func benchmarkWorkflowThroughput(
	b *testing.B,
	workflow cord.Workflow[int, int],
	concurrency int,
	nodesPerRun int,
) {
	b.Helper()
	b.ReportAllocs()

	var next atomic.Int64

	errors := make(chan error, concurrency)

	b.ResetTimer()

	var workers sync.WaitGroup
	for range concurrency {
		workers.Go(func() {
			for {
				index := next.Add(1)
				if index > int64(b.N) {
					return
				}

				if _, err := workflow.Run(b.Context(), int(index)); err != nil {
					errors <- err

					return
				}
			}
		})
	}

	workers.Wait()
	b.StopTimer()
	close(errors)

	for err := range errors {
		b.Fatalf("run benchmark workflow: %v", err)
	}

	seconds := b.Elapsed().Seconds()
	workflowRate := float64(b.N) / seconds
	nodeRate := float64(b.N*nodesPerRun) / seconds
	b.ReportMetric(workflowRate, "workflows/s")
	b.ReportMetric(nodeRate, "claims/s")
	b.ReportMetric(nodeRate, "completions/s")
}
