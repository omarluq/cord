package conformance

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func mustInspectRun(t *testing.T, backend storage.Backend, runID storage.RunID) storage.RunReport {
	t.Helper()

	report, err := backend.InspectRun(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}

	return report
}

func requireInspectionRun(
	t *testing.T,
	report *storage.RunReport,
	runID storage.RunID,
	state storage.RunStatus,
	reason storage.TerminalReason,
	counts storage.NodeStateCounts,
) {
	t.Helper()

	if report.ID != runID || report.State != state || report.Reason != reason || report.NodeCounts != counts {
		t.Fatalf("run report = %#v, want id=%q state=%q reason=%q counts=%#v",
			report, runID, state, reason, counts)
	}
}

func requireInitialRunReport(t *testing.T, report *storage.RunReport, workflowName string) {
	t.Helper()

	if report.WorkflowName != workflowName || report.FirstStartedAt != nil ||
		report.FinishedAt != nil || report.TerminalRunnerID != nil {
		t.Fatalf("initial run report = %#v", report)
	}

	requireUTC(t, "submitted", report.SubmittedAt)
	requireUTC(t, "state changed", report.StateChangedAt)
}

func requireStartedRunReport(t *testing.T, report *storage.RunReport) {
	t.Helper()

	if report.FirstStartedAt == nil {
		t.Fatal("claimed run has no first-start timestamp")
	}

	requireUTC(t, "first started", *report.FirstStartedAt)
}

func requireRunningNodeReport(t *testing.T, report *storage.NodeReport, claim *storage.Claim) {
	t.Helper()

	validState := report.State == storage.NodeRunning && report.Attempt == 1
	validRunner := report.RunnerID != nil && string(*report.RunnerID) == workerA
	validTimes := report.FirstStartedAt != nil && report.LastStartedAt != nil && report.StateChangedAt != nil

	if !validState || !validRunner || !validTimes {
		t.Fatalf("claimed node report = %#v, claim = %#v", report, claim)
	}

	requireCurrentLease(t, report, claim)
}

func requireCurrentLease(t *testing.T, report *storage.NodeReport, claim *storage.Claim) {
	t.Helper()

	if report.CurrentLease == nil {
		t.Fatalf("claimed node lease = %#v, claim = %#v", report, claim)
	}

	if string(report.CurrentLease.RunnerID) != workerA ||
		report.CurrentLease.Generation != claim.Lease.Generation {
		t.Fatalf("claimed node lease = %#v, claim = %#v", report, claim)
	}
}

func requireCompletedNodeReport(t *testing.T, report *storage.NodeReport) {
	t.Helper()

	if report.State != storage.NodeCompleted || report.Reason != storage.ReasonSucceeded ||
		report.FinishedAt == nil || report.CurrentLease != nil ||
		report.RunnerID == nil || *report.RunnerID != storage.RunnerID(workerA) {
		t.Fatalf("completed node report = %#v", report)
	}
}

func requireCanceledRunReport(t *testing.T, report *storage.RunReport) {
	t.Helper()

	if report.FinishedAt == nil || report.TerminalRunnerID != nil {
		t.Fatalf("canceled run report = %#v", report)
	}
}

func mustFindNode(
	t *testing.T,
	backend storage.Backend,
	runID storage.RunID,
	nodeID storage.NodeID,
) storage.NodeReport {
	t.Helper()

	var token string

	for {
		page, err := backend.ListRunNodes(t.Context(), runID, storage.NodeQuery{
			State: nil, Reason: nil, ContinuationToken: token, PageSize: 0,
		})
		if err != nil {
			t.Fatal(err)
		}

		for index := range page.Nodes {
			if page.Nodes[index].NodeID == nodeID {
				return page.Nodes[index]
			}
		}

		if page.ContinuationToken == "" {
			break
		}

		if page.ContinuationToken == token {
			t.Fatalf("continuation token did not advance from %q", token)
		}

		token = page.ContinuationToken
	}

	t.Fatalf("node %q missing from run %q", nodeID, runID)

	return storage.NodeReport{}
}

func requireUTC(t *testing.T, field string, instant time.Time) {
	t.Helper()

	if instant.IsZero() || instant.Location() != time.UTC {
		t.Fatalf("%s timestamp = %v, want nonzero UTC", field, instant)
	}
}
