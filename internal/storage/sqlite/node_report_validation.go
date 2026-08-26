package sqlite

import (
	"database/sql"
	"errors"

	"github.com/omarluq/cord/internal/storage"
)

func validateNodeReport(report *storage.NodeReport, validation *nodeReportValidation) error {
	terminal, err := validateNodeBasics(
		report, validation.leaseOwner, validation.leaseExpiresAt, validation.leaseGeneration,
	)
	if err != nil {
		return err
	}

	return validateCurrentNodeReport(report, validation.terminalReason, terminal)
}

func validateNodeBasics(
	report *storage.NodeReport,
	leaseOwner, leaseExpiresAt sql.NullString,
	leaseGeneration int64,
) (bool, error) {
	if err := validateNodeIdentity(report); err != nil {
		return false, err
	}

	terminal, _ := report.State.Terminal()
	if terminal != (report.FinishedAt != nil) {
		return false, errors.New("finish timestamp does not match node state")
	}

	if err := validateNodeLease(report, leaseOwner, leaseExpiresAt, leaseGeneration); err != nil {
		return false, err
	}

	if report.FirstStartedAt != nil && report.Attempt == 0 {
		return false, errors.New("unclaimed node has a first-start timestamp")
	}

	return terminal, nil
}

func validateNodeIdentity(report *storage.NodeReport) error {
	if report.RunID == "" || report.NodeID == "" || report.FunctionKey == "" ||
		report.EligibleAt.IsZero() || !report.State.IsKnown() {
		return errors.New("invalid node identity, timestamp, or state")
	}

	if report.MaxAttempts < 1 || report.Attempt < 0 || report.Attempt > report.MaxAttempts {
		return errors.New("invalid node attempt")
	}

	return nil
}

func validateNodeLease(
	report *storage.NodeReport,
	leaseOwner, leaseExpiresAt sql.NullString,
	leaseGeneration int64,
) error {
	if report.State == storage.NodeRunning {
		if report.CurrentLease == nil || report.CurrentLease.RunnerID == "" || leaseGeneration < 1 {
			return errors.New("running node has incomplete lease")
		}

		return nil
	}

	if leaseOwner.Valid || leaseExpiresAt.Valid || report.CurrentLease != nil {
		return errors.New("non-running node has a current lease")
	}

	return nil
}

func validateCurrentNodeReport(
	report *storage.NodeReport,
	reason sql.NullString,
	terminal bool,
) error {
	if err := validateCurrentNodeStart(report); err != nil {
		return err
	}

	if report.State == storage.NodeRunning &&
		(report.RunnerID == nil || report.CurrentLease.RunnerID != *report.RunnerID) {
		return errors.New("lease owner does not match latest runner")
	}

	if terminal && !report.FinishedAt.Equal(*report.StateChangedAt) {
		return errors.New("terminal state-change and finish timestamps differ")
	}

	return validateNodeReason(report, reason, terminal)
}

func validateCurrentNodeStart(report *storage.NodeReport) error {
	if report.StateChangedAt == nil {
		return errors.New("current node has no state-change timestamp")
	}

	if currentNodeStartIncomplete(report) {
		return errors.New("claimed node has incomplete start metadata")
	}

	if report.LastStartedAt != nil && report.FirstStartedAt == nil {
		return errors.New("last start exists without first start")
	}

	if report.LastStartedAt != nil && report.LastStartedAt.Before(*report.FirstStartedAt) {
		return errors.New("last start precedes first start")
	}

	if report.RunnerID != nil && (*report.RunnerID == "" || report.LastStartedAt == nil) {
		return errors.New("latest runner has invalid start metadata")
	}

	return nil
}

func currentNodeStartIncomplete(report *storage.NodeReport) bool {
	return report.Attempt > 0 &&
		(report.FirstStartedAt == nil || report.LastStartedAt == nil || report.RunnerID == nil)
}

func validateNodeReason(report *storage.NodeReport, reason sql.NullString, terminal bool) error {
	return validateReason(&report.Reason, reason, terminal, string(report.State), report.State.AllowsReason)
}
