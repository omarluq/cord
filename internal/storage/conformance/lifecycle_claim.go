package conformance

import (
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

const winningClaimMetadataFormat = "winner=%#v, persisted node=%#v"

type concurrentClaimResult struct {
	claim   *storage.Claim
	err     error
	owner   string
	claimed bool
}

type terminalRaceOutcome struct {
	result             *storage.RunResult
	node               *storage.NodeReport
	report             *storage.RunReport
	cancellation       storage.CancellationOutcome
	transitionAccepted bool
}

type terminalRaceExpectation struct {
	status storage.RunStatus
	owner  string
}

func runConcurrentClaimWinnerMetadata(t *testing.T, harness Harness) {
	t.Helper()

	opened := openStore(t, harness, "concurrent-claim-metadata")

	plan := singleNodePlan("conformance-concurrent-claim-metadata", "concurrent-claim-metadata")
	if err := opened.backend.CreateRun(t.Context(), &plan); err != nil {
		t.Fatal(err)
	}

	second, err := harness.NewBackend(opened.database)
	if err != nil {
		t.Fatal(err)
	}

	backends := []struct {
		backend storage.Backend
		owner   string
	}{
		{backend: opened.backend, owner: workerA},
		{backend: second, owner: workerB},
	}
	start := make(chan struct{})

	results := make(chan concurrentClaimResult, len(backends))
	for _, candidate := range backends {
		go func() {
			<-start

			claim, claimed, claimErr := claimAny(t.Context(), candidate.backend, candidate.owner)
			results <- concurrentClaimResult{
				claim: claim, claimed: claimed, err: claimErr, owner: candidate.owner,
			}
		}()
	}

	close(start)

	winner := requireSingleClaimWinner(t, results, len(backends))

	node := mustFindNode(t, opened.backend, plan.Run.ID, conformanceNodeID)
	requireWinningClaimMetadata(t, winner, &node)

	report := mustInspectRun(t, opened.backend, plan.Run.ID)
	if report.FirstStartedAt == nil {
		t.Fatalf("concurrently claimed run report = %#v", report)
	}
}

func requireWinningClaimMetadata(
	t *testing.T,
	winner *concurrentClaimResult,
	node *storage.NodeReport,
) {
	t.Helper()

	if winner.claim.Lease.Owner != winner.owner || node.State != storage.NodeRunning {
		t.Fatalf(winningClaimMetadataFormat, winner, node)
	}

	if node.RunnerID == nil || string(*node.RunnerID) != winner.owner {
		t.Fatalf(winningClaimMetadataFormat, winner, node)
	}

	if node.CurrentLease == nil || string(node.CurrentLease.RunnerID) != winner.owner {
		t.Fatalf(winningClaimMetadataFormat, winner, node)
	}

	if node.CurrentLease.Generation != winner.claim.Lease.Generation || node.Attempt != winner.claim.Attempt {
		t.Fatalf(winningClaimMetadataFormat, winner, node)
	}
}

func requireSingleClaimWinner(
	t *testing.T,
	results <-chan concurrentClaimResult,
	resultCount int,
) *concurrentClaimResult {
	t.Helper()

	var winner *concurrentClaimResult

	for range resultCount {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}

		if !result.claimed {
			if result.claim != nil {
				t.Fatalf("losing claim = %#v, want nil", result.claim)
			}

			continue
		}

		if result.claim == nil {
			t.Fatal("winning claim is nil")
		}

		if winner != nil {
			t.Fatalf("multiple concurrent claim winners: %#v and %#v", winner, result)
		}

		winner = &result
	}

	if winner == nil {
		t.Fatal("concurrent claim had no winner")
	}

	return winner
}
