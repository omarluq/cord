package cord_test

import (
	"context"
	"fmt"
	"testing"
)

func benchmarkPlanningPassThrough(_ context.Context, value int) (int, error) {
	return value, nil
}

func BenchmarkWorkflow_PlanningAndPersistence(b *testing.B) {
	for _, nodeCount := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			runtime := newBenchmarkRuntime(b, 1)

			workflow := runtime.From("benchmark-planning", benchmarkPlanningPassThrough)
			for range nodeCount - 1 {
				workflow = workflow.Then(benchmarkPlanningPassThrough)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for input := range b.N {
				if _, err := workflow.Run(b.Context(), input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
