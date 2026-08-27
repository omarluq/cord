package conformance

import (
	"reflect"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

type cancellationOrdering struct {
	second storage.Backend
	plan   *storage.RunPlan
	opened openedStore
}

func openCancellationOrdering(
	t *testing.T,
	harness Harness,
	name string,
) cancellationOrdering {
	t.Helper()

	opened := openStore(t, harness, "cancellation-ordering-"+name)

	plan := singleNodePlan(storage.RunID("conformance-cancellation-"+name), name)
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	second, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	return cancellationOrdering{opened: opened, plan: &plan, second: second}
}

func cancelRun(t *testing.T, backend storage.Backend, runID storage.RunID) {
	t.Helper()

	outcome, err := backend.CancelRun(t.Context(), runID)
	requireCancellationOutcome(t, outcome, err, storage.CancellationCanceled)
}

type nodeSnapshot struct {
	report storage.NodeReport
}

func mustNodeSnapshot(t *testing.T, backend storage.Backend, runID storage.RunID) nodeSnapshot {
	t.Helper()

	return nodeSnapshot{report: mustFindNode(t, backend, runID, conformanceNodeID)}
}

func requireNodeSnapshotUnchanged(
	t *testing.T,
	backend storage.Backend,
	runID storage.RunID,
	before *nodeSnapshot,
	operation string,
) {
	t.Helper()

	after := mustNodeSnapshot(t, backend, runID)
	if !reflect.DeepEqual(*before, after) {
		t.Fatalf("%s mutated node:\nbefore=%#v\nafter=%#v", operation, *before, after)
	}
}

type durableRunSnapshot struct {
	nodes      []storage.NodeReport
	nodeStates map[storage.NodeID]NodeState
	result     storage.RunResult
	report     storage.RunReport
}

func mustDurableRunSnapshot(
	t *testing.T,
	harness Harness,
	opened openedStore,
	runID storage.RunID,
) durableRunSnapshot {
	t.Helper()

	result, err := opened.backend.GetRunResult(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}

	states, err := harness.LoadNodeStates(t.Context(), opened.database, runID)
	if err != nil {
		t.Fatal(err)
	}

	return durableRunSnapshot{
		result:     result,
		report:     mustInspectRun(t, opened.backend, runID),
		nodes:      mustListRunNodes(t, opened.backend, runID),
		nodeStates: states,
	}
}

func mustListRunNodes(t *testing.T, backend storage.Backend, runID storage.RunID) []storage.NodeReport {
	t.Helper()

	page, err := backend.ListRunNodes(t.Context(), runID, storage.NodeQuery{})
	if err != nil {
		t.Fatal(err)
	}

	if page.ContinuationToken != "" {
		t.Fatalf("unexpected continuation token for durable snapshot: %q", page.ContinuationToken)
	}

	return page.Nodes
}

func requireDurableRunSnapshotUnchanged(
	t *testing.T,
	harness Harness,
	opened openedStore,
	runID storage.RunID,
	before *durableRunSnapshot,
	operation string,
) {
	t.Helper()

	after := mustDurableRunSnapshot(t, harness, opened, runID)
	if !reflect.DeepEqual(*before, after) {
		t.Fatalf("%s mutated durable run state:\nbefore=%#v\nafter=%#v", operation, *before, after)
	}
}

func assertCanceledNode(t *testing.T, backend storage.Backend, runID storage.RunID) {
	t.Helper()

	node := mustFindNode(t, backend, runID, conformanceNodeID)
	if node.State != storage.NodeCanceled || node.Reason != storage.ReasonCanceledByRequest ||
		node.CurrentLease != nil {
		t.Fatalf("canceled node = %#v", node)
	}
}

func runConcurrentCancellation(t *testing.T, harness Harness) {
	t.Helper()

	const testName = "concurrent-cancellation"

	opened := openStore(t, harness, testName)

	plan := singleNodePlan(testName, testName)
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	second, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	const concurrentCancellations = 2

	type cancelResult struct {
		err     error
		outcome storage.CancellationOutcome
	}

	start := make(chan struct{})
	results := make(chan cancelResult, concurrentCancellations)

	for _, backend := range []storage.Backend{opened.backend, second} {
		go func(backend storage.Backend) {
			<-start

			outcome, cancelErr := backend.CancelRun(t.Context(), plan.Run.ID)
			results <- cancelResult{outcome: outcome, err: cancelErr}
		}(backend)
	}

	close(start)

	counts := map[storage.CancellationOutcome]int{}

	for range concurrentCancellations {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}

		counts[result.outcome]++
	}

	if counts[storage.CancellationCanceled] != 1 || counts[storage.CancellationAlreadyCanceled] != 1 {
		t.Fatalf("concurrent cancellation outcomes = %#v", counts)
	}
}
