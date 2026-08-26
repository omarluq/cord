package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

const inspectRunQuery = `SELECT
		r.id, r.workflow_name, r.status, r.terminal_reason,
		r.created_at, r.started_at, r.updated_at, r.completed_at, r.terminal_runner_id,
		counts.pending, counts.ready, counts.running, counts.retry_wait,
		counts.completed, counts.failed, counts.canceled, counts.total
	FROM cord_runs r
	CROSS JOIN LATERAL (
		SELECT
			COUNT(*) FILTER (WHERE n.status = 'pending') AS pending,
			COUNT(*) FILTER (WHERE n.status = 'ready') AS ready,
			COUNT(*) FILTER (WHERE n.status = 'running') AS running,
			COUNT(*) FILTER (WHERE n.status = 'retry_wait') AS retry_wait,
			COUNT(*) FILTER (WHERE n.status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE n.status = 'failed') AS failed,
			COUNT(*) FILTER (WHERE n.status = 'canceled') AS canceled,
			COUNT(*) AS total
		FROM cord_nodes n
		WHERE n.run_id = r.id
	) counts
	WHERE r.id = $1`

// InspectRun returns one consistent, payload-free snapshot of a run and its
// node-state counts.
func (s *Store) InspectRun(ctx context.Context, runID storage.RunID) (storage.RunReport, error) {
	var (
		report         storage.RunReport
		reason         sql.NullString
		firstStartedAt sql.NullTime
		finishedAt     sql.NullTime
		terminalRunner sql.NullString
		totalNodes     int
	)

	err := s.pool.QueryRowContext(ctx, inspectRunQuery, runID).Scan(
		&report.ID,
		&report.WorkflowName,
		&report.State,
		&reason,
		&report.SubmittedAt,
		&firstStartedAt,
		&report.StateChangedAt,
		&finishedAt,
		&terminalRunner,
		&report.NodeCounts.Pending,
		&report.NodeCounts.Ready,
		&report.NodeCounts.Running,
		&report.NodeCounts.RetryWait,
		&report.NodeCounts.Completed,
		&report.NodeCounts.Failed,
		&report.NodeCounts.Canceled,
		&totalNodes,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.RunReport{}, fmt.Errorf("inspect run %q: %w", runID, storage.ErrRunNotFound)
	}

	if err != nil {
		return storage.RunReport{}, fmt.Errorf("inspect run %q: %w", runID, err)
	}

	report.SubmittedAt = report.SubmittedAt.UTC()
	report.StateChangedAt = report.StateChangedAt.UTC()
	report.FirstStartedAt = utcTime(firstStartedAt)
	report.FinishedAt = utcTime(finishedAt)

	if terminalRunner.Valid && terminalRunner.String == "" {
		return storage.RunReport{}, fmt.Errorf(
			"inspect run %q: %w", runID, incompatible("terminal runner is empty"),
		)
	}

	report.TerminalRunnerID = runnerID(terminalRunner)

	if err = validateRunReport(&report, reason, totalNodes); err != nil {
		return storage.RunReport{}, fmt.Errorf("inspect run %q: %w", runID, err)
	}

	return report, nil
}
