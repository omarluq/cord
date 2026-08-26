package sqlite

import (
	"database/sql"
	"fmt"
	"time"

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

func populateRunTimes(
	report *storage.RunReport,
	createdAt, updatedAt string,
	startedAt, completedAt sql.NullString,
) error {
	var err error
	if report.SubmittedAt, err = parseRequiredTime(createdAt); err != nil {
		return fmt.Errorf("invalid submitted timestamp: %w", err)
	}

	if report.StateChangedAt, err = parseRequiredTime(updatedAt); err != nil {
		return fmt.Errorf("invalid state-change timestamp: %w", err)
	}

	if err = parseOptionalTime(startedAt, &report.FirstStartedAt); err != nil {
		return fmt.Errorf("invalid first-start timestamp: %w", err)
	}

	if err = parseOptionalTime(completedAt, &report.FinishedAt); err != nil {
		return fmt.Errorf("invalid finish timestamp: %w", err)
	}

	return nil
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

func setOptionalRunnerID(destination **storage.RunnerID, value sql.NullString) {
	if value.Valid {
		runnerID := storage.RunnerID(value.String)
		*destination = &runnerID
	}
}

func parseRequiredTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse RFC3339 timestamp: %w", err)
	}

	return parsed.UTC(), nil
}

func parseOptionalTime(value sql.NullString, destination **time.Time) error {
	if !value.Valid {
		return nil
	}

	parsed, err := parseRequiredTime(value.String)
	if err != nil {
		return err
	}

	*destination = &parsed

	return nil
}

func incompatibleRun(runID storage.RunID, format string, arguments ...any) error {
	return fmt.Errorf("inspect run %q: %s: %w", runID, fmt.Sprintf(format, arguments...), storage.ErrRunIncompatible)
}

func incompatibleNode(
	runID storage.RunID,
	nodeID storage.NodeID,
	format string,
	arguments ...any,
) error {
	return fmt.Errorf(
		"inspect node %q for run %q: %s: %w",
		nodeID, runID, fmt.Sprintf(format, arguments...), storage.ErrRunIncompatible,
	)
}
