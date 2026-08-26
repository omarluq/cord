package cord

import "github.com/omarluq/cord/internal/storage"

func publicRunReport(report *storage.RunReport) (RunReport, error) {
	converted := RunReport{
		SubmittedAt:      report.SubmittedAt.UTC(),
		FirstStartedAt:   publicTime(report.FirstStartedAt),
		StateChangedAt:   report.StateChangedAt.UTC(),
		FinishedAt:       publicTime(report.FinishedAt),
		TerminalRunnerID: publicRunnerID(report.TerminalRunnerID),
		ID:               RunID(report.ID),
		WorkflowName:     report.WorkflowName,
		State:            RunState(report.State),
		Reason:           TerminalReason(report.Reason),
		NodeCounts: NodeStateCounts{
			Pending: report.NodeCounts.Pending, Ready: report.NodeCounts.Ready,
			Running: report.NodeCounts.Running, RetryWait: report.NodeCounts.RetryWait,
			Completed: report.NodeCounts.Completed, Failed: report.NodeCounts.Failed,
			Canceled: report.NodeCounts.Canceled,
		},
	}

	if err := validateRunReport(&converted); err != nil {
		return RunReport{}, err
	}

	return converted, nil
}

func validateRunReport(report *RunReport) error {
	if err := validateRunIdentity(report); err != nil {
		return err
	}

	if !report.State.AllowsReason(report.Reason) {
		return incompatibleSnapshot(
			"run %q has state %q with reason %q", report.ID, report.State, report.Reason,
		)
	}

	terminal, _ := report.State.Terminal()
	if terminal != (report.FinishedAt != nil) {
		return incompatibleSnapshot("run %q has inconsistent terminal timestamps", report.ID)
	}

	return validateRunMetadata(report)
}

func validateRunIdentity(report *RunReport) error {
	if report.ID == "" || report.WorkflowName == "" {
		return incompatibleSnapshot("run identity is empty")
	}

	if report.SubmittedAt.IsZero() || report.StateChangedAt.IsZero() {
		return incompatibleSnapshot("run %q has a missing required timestamp", report.ID)
	}

	return nil
}

func validateRunMetadata(report *RunReport) error {
	if err := validateTerminalRunner(report); err != nil {
		return err
	}

	counts := report.NodeCounts
	if counts.Pending < 0 || counts.Ready < 0 || counts.Running < 0 || counts.RetryWait < 0 ||
		counts.Completed < 0 || counts.Failed < 0 || counts.Canceled < 0 {
		return incompatibleSnapshot("run %q has a negative node count", report.ID)
	}

	return nil
}

func validateTerminalRunner(report *RunReport) error {
	if report.TerminalRunnerID != nil && *report.TerminalRunnerID == "" {
		return incompatibleSnapshot("run %q has an empty terminal runner ID", report.ID)
	}

	if report.State == RunStateCanceled && report.TerminalRunnerID != nil {
		return incompatibleSnapshot("canceled run %q has a terminal runner", report.ID)
	}

	if report.StateChangedAt.Before(report.SubmittedAt) ||
		(report.FirstStartedAt != nil && report.FirstStartedAt.Before(report.SubmittedAt)) {
		return incompatibleSnapshot("run %q has inconsistent lifecycle timestamps", report.ID)
	}

	if report.FinishedAt != nil && !report.FinishedAt.Equal(report.StateChangedAt) {
		return incompatibleSnapshot("run %q has inconsistent terminal timestamps", report.ID)
	}

	return nil
}
