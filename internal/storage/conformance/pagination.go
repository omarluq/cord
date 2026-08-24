package conformance

import (
	"errors"
	"fmt"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

func runNodePagination(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "node-pagination")

	plan := paginatedPlan("conformance-node-pagination")
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	requirePaginatedNodeIDs(t, opened.backend, &plan)
	requireFilteredNodes(t, opened.backend, &plan)
	requireMissingNodePage(t, opened.backend)
}

const firstPaginatedNodeID = storage.NodeID("node-00")

func requirePaginatedNodeIDs(t *testing.T, backend storage.Backend, plan *storage.RunPlan) {
	t.Helper()

	const pageSize = 2

	var (
		gotIDs []storage.NodeID
		token  string
	)

	for {
		page, err := backend.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
			State: nil, Reason: nil, ContinuationToken: token, PageSize: pageSize,
		})
		if err != nil {
			t.Fatal(err)
		}

		requireValidNodePage(t, &page, plan, token, pageSize)

		for index := range page.Nodes {
			gotIDs = append(gotIDs, page.Nodes[index].NodeID)
		}

		if page.ContinuationToken == "" {
			break
		}

		token = page.ContinuationToken
	}

	wantIDs := []storage.NodeID{firstPaginatedNodeID, "node-01", "node-02", "node-03", "node-04"}
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("paged node IDs = %v, want %v", gotIDs, wantIDs)
	}
}

func requireValidNodePage(
	t *testing.T,
	page *storage.NodePage,
	plan *storage.RunPlan,
	previousToken string,
	pageSize int,
) {
	t.Helper()

	if len(page.Nodes) > pageSize {
		t.Fatalf("page contains %d nodes, requested at most %d", len(page.Nodes), pageSize)
	}

	if len(page.Nodes) == 0 && page.ContinuationToken != "" {
		t.Fatalf("empty page has continuation token %q", page.ContinuationToken)
	}

	if page.ContinuationToken != "" && page.ContinuationToken == previousToken {
		t.Fatalf("continuation token did not advance from %q", previousToken)
	}

	for index := range page.Nodes {
		node := &page.Nodes[index]
		if node.RunID != plan.Run.ID || node.MaxAttempts != plan.Run.MaxAttempts {
			t.Fatalf("node report = %#v", node)
		}
	}
}

func requireFilteredNodes(t *testing.T, backend storage.Backend, plan *storage.RunPlan) {
	t.Helper()

	ready := storage.NodeReady

	readyPage, err := backend.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: &ready, Reason: nil, ContinuationToken: "", PageSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := nodeIDs(readyPage.Nodes); fmt.Sprint(got) != fmt.Sprint([]storage.NodeID{firstPaginatedNodeID}) {
		t.Fatalf("ready node IDs = %v", got)
	}

	noMatch := storage.NodeFailed

	empty, err := backend.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: &noMatch, Reason: nil, ContinuationToken: "", PageSize: 1,
	})
	if err != nil || len(empty.Nodes) != 0 || empty.ContinuationToken != "" {
		t.Fatalf("empty filtered page = %#v err=%v", empty, err)
	}

	claim := mustClaim(t, backend, workerA)
	accepted, err := backend.FailNode(
		t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`"failed"`),
		storage.ReasonFailureNonRetryable,
	)
	requireAccepted(t, "fail node for reason filter", accepted, err)

	reason := storage.ReasonFailureNonRetryable

	reasonPage, err := backend.ListRunNodes(t.Context(), plan.Run.ID, storage.NodeQuery{
		State: nil, Reason: &reason, ContinuationToken: "", PageSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := nodeIDs(reasonPage.Nodes); fmt.Sprint(got) != fmt.Sprint([]storage.NodeID{firstPaginatedNodeID}) {
		t.Fatalf("non-retryable failure node IDs = %v", got)
	}
}

func requireMissingNodePage(t *testing.T, backend storage.Backend) {
	t.Helper()

	_, err := backend.ListRunNodes(t.Context(), "missing-page-run", storage.NodeQuery{})
	if !errors.Is(err, storage.ErrRunNotFound) {
		t.Fatalf("ListRunNodes(missing) error = %v, want %v", err, storage.ErrRunNotFound)
	}
}

func runNodePaginationValidation(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "node-pagination-validation")

	plan := singleNodePlan("conformance-node-pagination-validation", "pagination-validation")
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	unknownState := storage.NodeStatus("future")
	unknownReason := storage.TerminalReason("future")
	completed := storage.NodeCompleted
	canceled := storage.ReasonCanceledByRequest

	const oversizedPage = storage.MaxNodePageSize + 1

	tests := []struct {
		name  string
		query storage.NodeQuery
	}{
		{name: "negative page size", query: nodeQuery(nil, nil, -1)},
		{name: "oversized page", query: nodeQuery(nil, nil, oversizedPage)},
		{name: "unknown state", query: nodeQuery(&unknownState, nil, 0)},
		{name: "unknown reason", query: nodeQuery(nil, &unknownReason, 0)},
		{name: "state reason mismatch", query: nodeQuery(&completed, &canceled, 0)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := opened.backend.ListRunNodes(t.Context(), plan.Run.ID, testCase.query); err == nil {
				t.Fatalf("ListRunNodes(%#v) returned nil error", testCase.query)
			}
		})
	}
}

func nodeQuery(state *storage.NodeStatus, reason *storage.TerminalReason, pageSize int) storage.NodeQuery {
	return storage.NodeQuery{
		State: state, Reason: reason, ContinuationToken: "", PageSize: pageSize,
	}
}

func paginatedPlan(runID storage.RunID) storage.RunPlan {
	const nodeCount = 5

	plan := singleNodePlan(runID, "node-pagination")
	plan.Run.TerminalNodeID = "node-04"
	plan.Nodes = make([]storage.Node, 0, nodeCount)
	plan.Edges = make([]storage.Edge, 0, nodeCount-1)

	for index := range nodeCount {
		nodeID := storage.NodeID(fmt.Sprintf("node-%02d", index))
		status := storage.NodePending
		remainingDependencies := 1

		if index == 0 {
			status = storage.NodeReady
			remainingDependencies = 0
		}

		plan.Nodes = append(plan.Nodes, conformanceNode(
			runID, nodeID, stepFunctionKey, "signature", status, plan.Run.CreatedAt, remainingDependencies,
		))
		if index > 0 {
			plan.Edges = append(plan.Edges, storage.Edge{
				RunID: runID, Parent: storage.NodeID(fmt.Sprintf("node-%02d", index-1)),
				Child: nodeID, ParentOrder: 0,
			})
		}
	}

	return plan
}

func nodeIDs(nodes []storage.NodeReport) []storage.NodeID {
	identifiers := make([]storage.NodeID, len(nodes))
	for index := range nodes {
		identifiers[index] = nodes[index].NodeID
	}

	return identifiers
}
