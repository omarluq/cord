package conformance

import (
	"errors"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

func runRestartAndResume(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "restart")
	database, store := opened.database, opened.backend

	plan := singleNodePlan("conformance-restart", "restart")
	if err := store.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	first := mustClaim(t, store, "departed-worker")
	if expireErr := harness.ExpireLease(t.Context(), database, first.RunID, first.NodeID); expireErr != nil {
		t.Fatal(expireErr)
	}

	restarted, err := harness.NewBackend(database)
	if err != nil {
		t.Fatal(err)
	}

	recovered, recoverErr := restarted.RecoverExpiredLeases(t.Context())
	requireSingleCount(t, "restart recovery", recovered, recoverErr)

	second := mustClaim(t, restarted, "resumed-worker")

	accepted, err := restarted.CompleteNode(t.Context(), second.RunID, second.NodeID, second.Lease, []byte(`"resumed"`))
	requireAccepted(t, "resume completion", accepted, err)
}

func runMigrationIdempotence(t *testing.T, harness Harness) {
	t.Helper()

	database := harness.Open(t, "migration-idempotence")
	if err := harness.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}

	if err := harness.Migrate(t.Context(), database); err != nil {
		t.Fatalf("second migration: %v", err)
	}

	if _, err := harness.NewBackend(database); err != nil {
		t.Fatal(err)
	}
}

func runRunDeletion(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "run-deletion")
	database, backend := opened.database, opened.backend

	plan := joinPlan("conformance-run-deletion")
	if err := backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	if err := harness.DeleteRun(t.Context(), database, plan.Run.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := backend.GetRunResult(t.Context(), plan.Run.ID); !errors.Is(err, storage.ErrRunNotFound) {
		t.Fatalf("deleted run error = %v, want %v", err, storage.ErrRunNotFound)
	}

	nodes, edges, err := harness.CountRunRows(t.Context(), database, plan.Run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if nodes != 0 || edges != 0 {
		t.Fatalf("rows after run deletion: nodes=%d edges=%d, want 0", nodes, edges)
	}
}
