package cord_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduler_ConcurrencyLimitIsGlobal(t *testing.T) {
	t.Parallel()

	for _, limit := range []int{1, 2} {
		t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
			t.Parallel()
			testGlobalConcurrencyLimit(t, limit)
		})
	}
}

func testGlobalConcurrencyLimit(t *testing.T, limit int) {
	t.Helper()

	database, runtime := newRuntime(t, cord.Options{Concurrency: limit})

	names := make([]string, limit+1)
	for index := range names {
		names[index] = fmt.Sprintf("workflow-%d", index)
	}

	probe := newBranchProbe(t, names...)

	results := make([]chan workflowResult, len(names))
	for index, name := range names {
		results[index] = make(chan workflowResult, 1)
		go runBlockingWorkflow(t.Context(), runtime, probe.server.URL, name, results[index])
	}

	started := make(map[string]bool, limit)
	for range limit {
		started[receiveStartedBranch(t, probe.started)] = true
	}

	assertRunningNodes(t, database, len(names), limit)
	assert.Equal(t, limit, probe.maximumActive())

	for name := range started {
		probe.release(name)

		break
	}

	last := receiveStartedBranch(t, probe.started)
	assert.False(t, started[last])

	for _, name := range names {
		probe.release(name)
	}

	for _, result := range results {
		require.NoError(t, waitWorkflowResult(t, result).err)
	}

	assert.Equal(t, limit, probe.maximumActive())
}

func runBlockingWorkflow(
	ctx context.Context,
	runtime *cord.Cord,
	probeURL string,
	name string,
	result chan<- workflowResult,
) {
	value, err := runtime.From(name, blockingWorkflow).Run(ctx, branchInput{ProbeURL: probeURL, Name: name})
	result <- workflowResult{value: value.Name, err: err}
}

func assertJoinPending(t *testing.T, database *sql.DB) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var (
			status                string
			remainingDependencies int
		)

		err := database.QueryRowContext(t.Context(), `SELECT status, remaining_deps
			FROM cord_nodes WHERE remaining_deps > 0`).Scan(&status, &remainingDependencies)
		if !assert.NoError(collect, err) {
			return
		}

		assert.Equal(collect, "pending", status)
		assert.Equal(collect, 1, remainingDependencies)
	}, 5*time.Second, 10*time.Millisecond)
}

func assertRunningNodes(t *testing.T, database *sql.DB, wantRuns, wantRunning int) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var (
			runs    int
			running int
		)

		if !assert.NoError(collect, database.QueryRowContext(
			t.Context(), "SELECT COUNT(*) FROM cord_runs",
		).Scan(&runs)) {
			return
		}

		if !assert.NoError(collect, database.QueryRowContext(
			t.Context(), "SELECT COUNT(*) FROM cord_nodes WHERE status = 'running'",
		).Scan(&running)) {
			return
		}

		assert.Equal(collect, wantRuns, runs)
		assert.Equal(collect, wantRunning, running)
	}, 5*time.Second, 10*time.Millisecond)
}

func receiveStartedBranch(t *testing.T, started <-chan string) string {
	t.Helper()

	select {
	case name := <-started:
		return name
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for branch to start")

		return ""
	}
}
