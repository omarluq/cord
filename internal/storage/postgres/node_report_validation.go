package postgres

import (
	"database/sql"

	"github.com/omarluq/cord/internal/storage"
)

type nodeValidation struct {
	leaseExpiresAt sql.NullTime
	runStatus      storage.RunStatus
	reason         sql.NullString
	lastRunner     sql.NullString
	leaseOwner     sql.NullString
}

func validateNodeReport(report *storage.NodeReport, validation *nodeValidation) error {
	if err := validateNodeMetadata(report); err != nil {
		return err
	}

	terminal, _ := report.State.Terminal()
	if validation.reason.Valid {
		report.Reason = storage.TerminalReason(validation.reason.String)
	}

	if err := validateNodeReason(report, validation.reason, terminal); err != nil {
		return err
	}

	return validateNodeStateFields(report, validation, terminal)
}

func validateNodeMetadata(report *storage.NodeReport) error {
	if report.RunID == "" || report.NodeID == "" || report.FunctionKey == "" || report.EligibleAt.IsZero() {
		return incompatible("missing required node metadata")
	}

	if !report.State.IsKnown() || report.MaxAttempts < 1 || report.Attempt < 0 || report.Attempt > report.MaxAttempts {
		return incompatible("invalid node state or attempt metadata")
	}

	return nil
}

func validateNodeReason(
	report *storage.NodeReport,
	reason sql.NullString,
	terminal bool,
) error {
	if !report.State.AllowsReason(report.Reason) || (report.Reason != "" && !report.Reason.IsKnown()) {
		return incompatible("node state %q has invalid reason %q", report.State, report.Reason)
	}

	if terminal != reason.Valid || report.StateChangedAt == nil {
		return incompatible("node state, reason, and state-change time disagree")
	}

	return nil
}

func validateNodeStateFields(
	report *storage.NodeReport,
	validation *nodeValidation,
	terminal bool,
) error {
	if terminal != (report.FinishedAt != nil) {
		return incompatible("node terminal state and finish time disagree")
	}

	if err := validateNodeLease(report, validation); err != nil {
		return err
	}

	return validateNodeTimestamps(report, terminal)
}

func validateNodeLease(report *storage.NodeReport, validation *nodeValidation) error {
	if report.State != storage.NodeRunning {
		if validation.leaseOwner.Valid || validation.leaseExpiresAt.Valid {
			return incompatible("non-running node retains an active lease")
		}

		return nil
	}

	if !nodeLeaseComplete(report, validation) {
		return incompatible("running node has no complete lease")
	}

	if !nodeRunnerMatchesLease(report) {
		return incompatible("running node lease and latest runner disagree")
	}

	return nil
}

func nodeLeaseComplete(report *storage.NodeReport, validation *nodeValidation) bool {
	return validation.leaseOwner.Valid && validation.leaseOwner.String != "" &&
		validation.leaseExpiresAt.Valid && report.CurrentLease != nil && report.CurrentLease.Generation >= 1
}

func nodeRunnerMatchesLease(report *storage.NodeReport) bool {
	return report.RunnerID != nil && *report.RunnerID == report.CurrentLease.RunnerID && report.LastStartedAt != nil
}

func validateNodeTimestamps(report *storage.NodeReport, terminal bool) error {
	if err := validateNodeStarts(report); err != nil {
		return err
	}

	if report.LastStartedAt != nil && report.LastStartedAt.After(*report.StateChangedAt) {
		return incompatible("node latest start follows current state entry")
	}

	if report.StateChangedAt.Before(report.EligibleAt) && report.Attempt == 0 {
		return incompatible("unclaimed node state-change time precedes eligibility")
	}

	if terminal && !report.FinishedAt.Equal(*report.StateChangedAt) {
		return incompatible("node finish and state-change times differ")
	}

	return nil
}

func validateNodeStarts(report *storage.NodeReport) error {
	if report.FirstStartedAt != nil && report.Attempt == 0 {
		return incompatible("unclaimed node has a first-start time")
	}

	if report.LastStartedAt != nil && report.FirstStartedAt == nil {
		return incompatible("node latest start exists without first start")
	}

	if report.LastStartedAt != nil && report.LastStartedAt.Before(*report.FirstStartedAt) {
		return incompatible("node latest start precedes first start")
	}

	if (report.RunnerID == nil) != (report.LastStartedAt == nil) {
		return incompatible("node latest runner and start disagree")
	}

	if report.Attempt > 0 && !nodeStartMetadataComplete(report) {
		return incompatible("claimed node has incomplete start metadata")
	}

	return nil
}

func nodeStartMetadataComplete(report *storage.NodeReport) bool {
	return report.FirstStartedAt != nil && report.LastStartedAt != nil && report.RunnerID != nil
}
