package cord_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	branchLeft  = "left"
	branchRight = "right"
)

type branchInput struct {
	// ProbeURL is the URL of the branch synchronization probe.
	ProbeURL string
	// Name identifies the workflow to the probe.
	Name string
}

type branchOutput struct {
	// Name identifies the completed branch.
	Name string
}

type branchGate struct {
	done chan struct{}
	once sync.Once
}

type branchProbe struct {
	server  *httptest.Server
	started chan string
	gates   map[string]*branchGate
	mu      sync.Mutex
	active  int
	maximum int
}

func newBranchProbe(t *testing.T, names ...string) *branchProbe {
	t.Helper()

	gates := make(map[string]*branchGate, len(names))
	for _, name := range names {
		gates[name] = &branchGate{done: make(chan struct{})}
	}

	probe := &branchProbe{
		started: make(chan string, len(names)),
		gates:   gates,
	}
	probe.server = httptest.NewServer(http.HandlerFunc(probe.handleBranch))

	t.Cleanup(func() {
		for name := range probe.gates {
			probe.release(name)
		}

		probe.server.Close()
	})

	return probe
}

func (p *branchProbe) handleBranch(response http.ResponseWriter, request *http.Request) {
	name := strings.TrimPrefix(request.URL.Path, "/")

	gate, ok := p.gates[name]
	if !ok {
		http.Error(response, "unknown branch", http.StatusNotFound)

		return
	}

	p.mu.Lock()
	p.active++
	p.maximum = max(p.maximum, p.active)
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
	}()

	p.started <- name

	select {
	case <-request.Context().Done():
		http.Error(response, request.Context().Err().Error(), http.StatusRequestTimeout)
	case <-gate.done:
		response.WriteHeader(http.StatusNoContent)
	}
}

func (p *branchProbe) release(name string) {
	p.gates[name].once.Do(func() { close(p.gates[name].done) })
}

func (p *branchProbe) maximumActive() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.maximum
}

func branchRoot(_ context.Context, input branchInput) (branchInput, error) {
	return input, nil
}

func leftBranch(ctx context.Context, input branchInput) (branchOutput, error) {
	return runBranch(ctx, input.ProbeURL, branchLeft)
}

func rightBranch(ctx context.Context, input branchInput) (branchOutput, error) {
	return runBranch(ctx, input.ProbeURL, branchRight)
}

func blockingWorkflow(ctx context.Context, input branchInput) (branchOutput, error) {
	return runBranch(ctx, input.ProbeURL, input.Name)
}

func runBranch(ctx context.Context, probeURL, name string) (branchOutput, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, probeURL+"/"+name, http.NoBody)
	if err != nil {
		return branchOutput{}, fmt.Errorf("create %s branch request: %w", name, err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return branchOutput{}, fmt.Errorf("execute %s branch request: %w", name, err)
	}

	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()

	if err := errors.Join(readErr, closeErr); err != nil {
		return branchOutput{}, fmt.Errorf("consume %s branch response: %w", name, err)
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return branchOutput{}, fmt.Errorf("execute %s branch request: unexpected HTTP status %s", name, response.Status)
	}

	return branchOutput{Name: name}, nil
}

func joinBranches(_ context.Context, left, right branchOutput) (string, error) {
	return left.Name + ":" + right.Name, nil
}

func TestScheduler_JoinedBranchesSynchronizeAndPreserveOrder(t *testing.T) {
	t.Parallel()

	for _, first := range []string{branchLeft, branchRight} {
		t.Run(first+" completes first", func(t *testing.T) {
			t.Parallel()

			database, runtime := newRuntime(t, cord.Options{Concurrency: 2})
			probe := newBranchProbe(t, branchLeft, branchRight)
			root := runtime.From("branch-concurrency", branchRoot)
			workflow := cord.Join(root.Then(leftBranch), root.Then(rightBranch)).Then(joinBranches)
			result := make(chan workflowResult, 1)

			go func() {
				value, err := workflow.Run(t.Context(), branchInput{ProbeURL: probe.server.URL})
				result <- workflowResult{value: value, err: err}
			}()

			started := map[string]bool{
				receiveStartedBranch(t, probe.started): true,
				receiveStartedBranch(t, probe.started): true,
			}
			assert.True(t, started[branchLeft])
			assert.True(t, started[branchRight])
			assert.Equal(t, 2, probe.maximumActive())

			probe.release(first)
			assertJoinPending(t, database)
			assert.Empty(t, result)

			second := branchLeft
			if first == branchLeft {
				second = branchRight
			}

			probe.release(second)

			completed := waitWorkflowResult(t, result)
			require.NoError(t, completed.err)
			assert.Equal(t, branchLeft+":"+branchRight, completed.value)
		})
	}
}

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
		require.NoError(collect, err)
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

		require.NoError(collect, database.QueryRowContext(
			t.Context(), "SELECT COUNT(*) FROM cord_runs",
		).Scan(&runs))
		require.NoError(collect, database.QueryRowContext(
			t.Context(), "SELECT COUNT(*) FROM cord_nodes WHERE status = 'running'",
		).Scan(&running))
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
