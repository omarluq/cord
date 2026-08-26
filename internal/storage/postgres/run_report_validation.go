package postgres

import (
	"database/sql"

	"github.com/omarluq/cord/internal/storage"
)

func validateRunReport(
	report *storage.RunReport,
	reason sql.NullString,
	totalNodes int,
) error {
	if err := validateRunMetadata(report, totalNodes); err != nil {
		return err
	}

	terminal, _ := report.State.Terminal()

	if reason.Valid {
		report.Reason = storage.TerminalReason(reason.String)
	}

	if err := validateRunReason(report, reason, terminal); err != nil {
		return err
	}

	return validateRunTimestamps(report, terminal)
}

func validateRunMetadata(report *storage.RunReport, totalNodes int) error {
	if report.ID == "" || report.WorkflowName == "" || report.SubmittedAt.IsZero() || report.StateChangedAt.IsZero() {
		return incompatible("missing required run metadata")
	}

	if !report.State.IsKnown() {
		return incompatible("unknown run state %q", report.State)
	}

	if totalNodes == 0 || totalNodes != sumNodeCounts(report.NodeCounts) {
		return incompatible("node-state counts are incomplete")
	}

	return nil
}

func validateRunReason(report *storage.RunReport, reason sql.NullString, terminal bool) error {
	if !report.State.AllowsReason(report.Reason) || (report.Reason != "" && !report.Reason.IsKnown()) {
		return incompatible("run state %q has invalid reason %q", report.State, report.Reason)
	}

	if terminal != reason.Valid {
		return incompatible("run terminal state and reason disagree")
	}

	return validateTerminalRunner(report.Reason, report.TerminalRunnerID != nil)
}

func validateTerminalRunner(reason storage.TerminalReason, claimed bool) error {
	switch reason {
	case storage.ReasonSucceeded,
		storage.ReasonFailureNonRetryable,
		storage.ReasonFailureAttemptsExhausted:
		if !claimed {
			return incompatible("claimed terminal transition has no terminal runner")
		}
	case storage.ReasonCanceledByRequest, storage.ReasonFailureLeaseExpired, "":
		if claimed {
			return incompatible("unclaimed terminal transition has a terminal runner")
		}
	case storage.ReasonCanceledByRunFailure:
		return incompatible("run has node-only terminal reason")
	}

	return nil
}

func validateRunTimestamps(report *storage.RunReport, terminal bool) error {
	if terminal != (report.FinishedAt != nil) {
		return incompatible("run terminal state and finish time disagree")
	}

	if report.StateChangedAt.Before(report.SubmittedAt) {
		return incompatible("run state-change time precedes submission")
	}

	if report.FirstStartedAt != nil && report.FirstStartedAt.Before(report.SubmittedAt) {
		return incompatible("run first-start time precedes submission")
	}

	executionTerminal := report.State == storage.RunCompleted || report.State == storage.RunFailed
	if executionTerminal && report.FirstStartedAt == nil {
		return incompatible("execution-terminal run has no first-start time")
	}

	if terminal && !report.FinishedAt.Equal(report.StateChangedAt) {
		return incompatible("run finish and state-change times differ")
	}

	return nil
}
