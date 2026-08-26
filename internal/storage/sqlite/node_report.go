package sqlite

import (
	"database/sql"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

type rowScanner interface {
	Scan(...any) error
}

type nodeReportValidation struct {
	runStatus       storage.RunStatus
	terminalReason  sql.NullString
	lastRunnerID    sql.NullString
	leaseOwner      sql.NullString
	leaseExpiresAt  sql.NullString
	leaseGeneration int64
}

func scanNodeReport(
	rows rowScanner,
	runID storage.RunID,
	runStatus storage.RunStatus,
	maxAttempts int,
) (storage.NodeReport, error) {
	var (
		report                                                   storage.NodeReport
		availableAt                                              string
		startedAt, lastStartedAt, stateChangedAt, completedAt    sql.NullString
		terminalReason, lastRunnerID, leaseOwner, leaseExpiresAt sql.NullString
		leaseGeneration                                          int64
	)

	report.RunID = runID
	report.MaxAttempts = maxAttempts

	if err := rows.Scan(
		&report.NodeID, &report.FunctionKey, &report.State, &report.Attempt,
		&availableAt, &startedAt, &lastStartedAt, &stateChangedAt, &completedAt,
		&terminalReason, &lastRunnerID,
		&leaseOwner, &leaseGeneration, &leaseExpiresAt,
	); err != nil {
		return report, fmt.Errorf("scan node for run %q: %w", runID, err)
	}

	if err := populateNodeTimes(
		&report, availableAt, startedAt, lastStartedAt, stateChangedAt, completedAt,
	); err != nil {
		return report, incompatibleNode(runID, report.NodeID, "%v", err)
	}

	setOptionalRunnerID(&report.RunnerID, lastRunnerID)

	if err := populateCurrentLease(&report, leaseOwner, leaseExpiresAt, leaseGeneration); err != nil {
		return report, incompatibleNode(runID, report.NodeID, "%v", err)
	}

	validation := nodeReportValidation{
		runStatus:       runStatus,
		terminalReason:  terminalReason,
		lastRunnerID:    lastRunnerID,
		leaseOwner:      leaseOwner,
		leaseExpiresAt:  leaseExpiresAt,
		leaseGeneration: leaseGeneration,
	}
	if err := validateNodeReport(&report, &validation); err != nil {
		return storage.NodeReport{}, incompatibleNode(runID, report.NodeID, "%v", err)
	}

	return report, nil
}

func populateNodeTimes(
	report *storage.NodeReport,
	availableAt string,
	startedAt, lastStartedAt, stateChangedAt, completedAt sql.NullString,
) error {
	var err error
	if report.EligibleAt, err = parseRequiredTime(availableAt); err != nil {
		return fmt.Errorf("invalid eligible timestamp: %w", err)
	}

	if err = parseOptionalTime(startedAt, &report.FirstStartedAt); err != nil {
		return fmt.Errorf("invalid first-start timestamp: %w", err)
	}

	if err = parseOptionalTime(lastStartedAt, &report.LastStartedAt); err != nil {
		return fmt.Errorf("invalid last-start timestamp: %w", err)
	}

	if err = parseOptionalTime(stateChangedAt, &report.StateChangedAt); err != nil {
		return fmt.Errorf("invalid state-change timestamp: %w", err)
	}

	if err = parseOptionalTime(completedAt, &report.FinishedAt); err != nil {
		return fmt.Errorf("invalid finish timestamp: %w", err)
	}

	return nil
}

func populateCurrentLease(
	report *storage.NodeReport,
	leaseOwner, leaseExpiresAt sql.NullString,
	leaseGeneration int64,
) error {
	if report.State != storage.NodeRunning || !leaseOwner.Valid || !leaseExpiresAt.Valid {
		return nil
	}

	expiresAt, err := parseRequiredTime(leaseExpiresAt.String)
	if err != nil {
		return fmt.Errorf("invalid lease expiry: %w", err)
	}

	report.CurrentLease = &storage.CurrentLease{
		ExpiresAt:  expiresAt,
		RunnerID:   storage.RunnerID(leaseOwner.String),
		Generation: leaseGeneration,
	}

	return nil
}
