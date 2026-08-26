package conformance

import (
	"errors"
	"fmt"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

func runCreateAndResult(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "create-result")
	store := opened.backend
	plan := singleNodePlan("conformance-create", "create")

	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	result, err := store.GetRunResult(t.Context(), plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	requireRunResult(t, &result, storage.RunRunning, nil, nil)
	requireRunIdentity(t, &result, &plan)

	claim := mustClaim(t, store, "worker")

	const output = `"completed"`

	accepted, err := store.CompleteNode(t.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(output))
	requireAccepted(t, "complete node", accepted, err)

	result, err = store.GetRunResult(t.Context(), plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	requireRunResult(t, &result, storage.RunCompleted, []byte(output), nil)
	requireRunIdentity(t, &result, &plan)

	_, err = store.GetRunResult(t.Context(), "missing-run")
	if !errors.Is(err, storage.ErrRunNotFound) {
		t.Fatalf("missing result error = %v, want %v", err, storage.ErrRunNotFound)
	}
}

func runInvalidRunPlans(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "invalid-run-plans")

	for index, testCase := range ValidationRunPlanTests() {
		if testCase.WantErr == "" {
			continue
		}

		t.Run(testCase.Name, func(t *testing.T) {
			runInvalidRunPlan(t, harness, opened, testCase, index)
		})
	}
}

func runInvalidRunPlan(
	t *testing.T,
	harness Harness,
	opened openedStore,
	testCase RunPlanValidationTest,
	index int,
) {
	t.Helper()

	plan := ValidationJoinPlan(storage.RunID(fmt.Sprintf("invalid-plan-%d", index)))
	testCase.Mutate(&plan)

	err := opened.backend.CreateRun(t.Context(), &plan)
	wantErr := "create run: " + testCase.WantErr

	if err == nil || err.Error() != wantErr {
		t.Fatalf("CreateRun() error = %v, want %q", err, wantErr)
	}

	_, resultErr := opened.backend.GetRunResult(t.Context(), plan.Run.ID)
	if !errors.Is(resultErr, storage.ErrRunNotFound) {
		t.Fatalf("GetRunResult() error = %v, want %v", resultErr, storage.ErrRunNotFound)
	}

	nodes, edges, countErr := harness.CountRunRows(t.Context(), opened.database, plan.Run.ID)
	if countErr != nil {
		t.Fatal(countErr)
	}

	if nodes != 0 || edges != 0 {
		t.Fatalf("invalid plan persisted rows: nodes=%d edges=%d", nodes, edges)
	}
}
