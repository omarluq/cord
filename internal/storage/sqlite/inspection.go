package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

// InspectRun reads one consistent, payload-free snapshot of a run and its node counts.
func (s *Store) InspectRun(ctx context.Context, runID storage.RunID) (storage.RunReport, error) {
	var (
		report                           storage.RunReport
		createdAt, updatedAt             string
		startedAt, completedAt           sql.NullString
		terminalReason, terminalRunnerID sql.NullString
		totalNodes, knownNodes           int
	)

	err := s.database.QueryRowContext(ctx, `SELECT
		r.id, r.workflow_name, r.status, r.created_at, r.updated_at,
		r.started_at, r.completed_at,
		r.terminal_reason, r.terminal_runner_id,
		COUNT(n.node_id),
		COALESCE(SUM(CASE WHEN n.status IN (?, ?, ?, ?, ?, ?, ?) THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN n.status = ? THEN 1 ELSE 0 END), 0)
	FROM cord_runs AS r
	LEFT JOIN cord_nodes AS n ON n.run_id = r.id
	WHERE r.id = ?
	GROUP BY r.id`,
		storage.NodePending, storage.NodeReady, storage.NodeRunning, storage.NodeRetryWait,
		storage.NodeCompleted, storage.NodeFailed, storage.NodeCanceled,
		storage.NodePending, storage.NodeReady, storage.NodeRunning, storage.NodeRetryWait,
		storage.NodeCompleted, storage.NodeFailed, storage.NodeCanceled, runID,
	).Scan(
		&report.ID, &report.WorkflowName, &report.State, &createdAt, &updatedAt,
		&startedAt, &completedAt, &terminalReason, &terminalRunnerID,
		&totalNodes, &knownNodes,
		&report.NodeCounts.Pending, &report.NodeCounts.Ready, &report.NodeCounts.Running,
		&report.NodeCounts.RetryWait, &report.NodeCounts.Completed, &report.NodeCounts.Failed,
		&report.NodeCounts.Canceled,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return report, fmt.Errorf("inspect run %q: %w", runID, storage.ErrRunNotFound)
	}

	if err != nil {
		return report, fmt.Errorf("inspect run %q: %w", runID, err)
	}

	if totalNodes != knownNodes {
		return report, incompatibleRun(runID, "unknown node state")
	}

	if totalNodes == 0 {
		return report, incompatibleRun(runID, "run has no nodes")
	}

	if err = populateRunTimes(&report, createdAt, updatedAt, startedAt, completedAt); err != nil {
		return report, incompatibleRun(runID, "%v", err)
	}

	setOptionalRunnerID(&report.TerminalRunnerID, terminalRunnerID)

	if err = validateRunReport(&report, terminalReason); err != nil {
		return storage.RunReport{}, incompatibleRun(runID, "%v", err)
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

func validateRunReport(report *storage.RunReport, reason sql.NullString) error {
	terminal, err := validateRunBasics(report)
	if err != nil {
		return err
	}

	return validateCurrentRunReport(report, reason, terminal)
}

func validateRunBasics(report *storage.RunReport) (bool, error) {
	if report.ID == "" || report.WorkflowName == "" ||
		report.SubmittedAt.IsZero() || report.StateChangedAt.IsZero() || !report.State.IsKnown() {
		return false, errors.New("invalid run identity, timestamp, or state")
	}

	terminal, _ := report.State.Terminal()
	if terminal != (report.FinishedAt != nil) {
		return false, errors.New("finish timestamp does not match run state")
	}

	if report.FirstStartedAt != nil && report.FirstStartedAt.Before(report.SubmittedAt) {
		return false, errors.New("first start precedes submission")
	}

	return terminal, nil
}

func validateCurrentRunReport(
	report *storage.RunReport,
	reason sql.NullString,
	terminal bool,
) error {
	if report.StateChangedAt.Before(report.SubmittedAt) {
		return errors.New("state change precedes submission")
	}

	if terminal && !report.FinishedAt.Equal(report.StateChangedAt) {
		return errors.New("terminal state-change and finish timestamps differ")
	}

	if err := validateRunReason(report, reason, terminal); err != nil {
		return err
	}

	return validateTerminalRunner(report, terminal)
}

func validateTerminalRunner(report *storage.RunReport, terminal bool) error {
	if report.TerminalRunnerID != nil && (*report.TerminalRunnerID == "" || !terminal) {
		return errors.New("terminal runner is invalid")
	}

	if report.State == storage.RunCanceled && report.TerminalRunnerID != nil {
		return errors.New("canceled run has a terminal runner")
	}

	return nil
}

func validateRunReason(report *storage.RunReport, reason sql.NullString, terminal bool) error {
	return validateReason(&report.Reason, reason, terminal, string(report.State), report.State.AllowsReason)
}
