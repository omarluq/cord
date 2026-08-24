package conformance

import (
	"errors"
	"fmt"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

const testIdempotencyKey = "order-42"

func runIdempotentCreateOrAttach(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "idempotent-create-attach")
	first := keyedPlan("idempotent-first", testIdempotencyKey)

	runID, created, err := opened.backend.CreateOrAttachRun(t.Context(), &first)
	requireCreateResult(t, runID, created, err, first.Run.ID, true)

	attached := keyedPlan("idempotent-attached-candidate", testIdempotencyKey)
	runID, created, err = opened.backend.CreateOrAttachRun(t.Context(), &attached)
	requireCreateResult(t, runID, created, err, first.Run.ID, false)

	if _, err = opened.backend.GetRunResult(t.Context(), attached.Run.ID); !errors.Is(err, storage.ErrRunNotFound) {
		t.Fatalf("attach candidate persisted: error = %v, want %v", err, storage.ErrRunNotFound)
	}

	nodes, edges, err := harness.CountRunRows(t.Context(), opened.database, attached.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if nodes != 0 || edges != 0 {
		t.Fatalf("attached candidate rows: nodes=%d edges=%d, want 0", nodes, edges)
	}
}

func runIdempotentCreateConflict(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "idempotent-conflict")

	retained := keyedPlan("conflict-retained", testIdempotencyKey)
	if _, _, err := opened.backend.CreateOrAttachRun(t.Context(), &retained); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		mutate func(*storage.RunPlan)
		name   string
	}{
		{name: "submission fingerprint", mutate: func(plan *storage.RunPlan) {
			plan.Run.SubmissionFingerprint = new("different-submission")
		}},
		{name: "definition hash", mutate: func(plan *storage.RunPlan) {
			plan.Run.DefinitionHash = "different-definition"
		}},
	}

	for index, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := keyedPlan(
				storage.RunID(fmt.Sprintf("conflict-candidate-%d", index)), testIdempotencyKey,
			)
			testCase.mutate(&candidate)

			runID, created, err := opened.backend.CreateOrAttachRun(t.Context(), &candidate)
			if !errors.Is(err, storage.ErrRunConflict) || runID != "" || created {
				t.Fatalf("conflicting create = (%q, %v, %v), want (empty, false, %v)",
					runID, created, err, storage.ErrRunConflict)
			}

			_, resultErr := opened.backend.GetRunResult(t.Context(), candidate.Run.ID)
			if !errors.Is(resultErr, storage.ErrRunNotFound) {
				t.Fatalf("conflicting candidate persisted: %v", resultErr)
			}
		})
	}

	result, err := opened.backend.GetRunResult(t.Context(), retained.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	requireRunIdentity(t, &result, &retained)
}

func runConcurrentIdempotentCreate(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "concurrent-idempotent-create")

	secondBackend, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	plans := []storage.RunPlan{
		keyedPlan("concurrent-idempotent-a", "order-concurrent"),
		keyedPlan("concurrent-idempotent-b", "order-concurrent"),
	}
	backends := []storage.Backend{opened.backend, secondBackend}

	start := make(chan struct{})

	results := make(chan createResult, len(plans))
	for index := range plans {
		go func(index int) {
			<-start

			runID, created, createErr := backends[index].CreateOrAttachRun(t.Context(), &plans[index])
			results <- createResult{runID: runID, created: created, err: createErr}
		}(index)
	}

	close(start)

	requireConcurrentCreateResults(t, results, len(plans))
	requireSinglePersistedPlan(t, opened.backend, plans)
}

type createResult struct {
	err     error
	runID   storage.RunID
	created bool
}

func requireConcurrentCreateResults(t *testing.T, results <-chan createResult, resultCount int) {
	t.Helper()

	var retained storage.RunID

	createdCount := 0

	for range resultCount {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}

		if result.created {
			createdCount++
		}

		if retained == "" {
			retained = result.runID
		} else if result.runID != retained {
			t.Fatalf("concurrent create run IDs differ: %q and %q", retained, result.runID)
		}
	}

	if createdCount != 1 {
		t.Fatalf("concurrent newly-created count = %d, want 1", createdCount)
	}
}

func requireSinglePersistedPlan(t *testing.T, backend storage.Backend, plans []storage.RunPlan) {
	t.Helper()

	persistedCandidates := 0

	for index := range plans {
		if _, err := backend.GetRunResult(t.Context(), plans[index].Run.ID); err == nil {
			persistedCandidates++
		} else if !errors.Is(err, storage.ErrRunNotFound) {
			t.Fatal(err)
		}
	}

	if persistedCandidates != 1 {
		t.Fatalf("persisted concurrent candidates = %d, want 1", persistedCandidates)
	}
}

func runIdempotencyDeletionRelease(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "idempotency-deletion-release")

	first := keyedPlan("deletion-first", "reusable-key")
	if _, _, err := opened.backend.CreateOrAttachRun(t.Context(), &first); err != nil {
		t.Fatal(err)
	}

	if err := harness.DeleteRun(t.Context(), opened.database, first.Run.ID); err != nil {
		t.Fatal(err)
	}

	second := keyedPlan("deletion-second", "reusable-key")
	runID, created, err := opened.backend.CreateOrAttachRun(t.Context(), &second)
	requireCreateResult(t, runID, created, err, second.Run.ID, true)
}

func keyedPlan(runID storage.RunID, key string) storage.RunPlan {
	plan := singleNodePlan(runID, "orders")
	plan.Run.IdempotencyKey = new(key)
	plan.Run.SubmissionFingerprint = new("submission-v1")

	return plan
}

func requireCreateResult(
	t *testing.T,
	runID storage.RunID,
	created bool,
	err error,
	wantRunID storage.RunID,
	wantCreated bool,
) {
	t.Helper()

	if err != nil || runID != wantRunID || created != wantCreated {
		t.Fatalf("CreateRun() = (%q, %v, %v), want (%q, %v, nil)",
			runID, created, err, wantRunID, wantCreated)
	}
}
