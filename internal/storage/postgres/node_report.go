package postgres

import (
	"database/sql"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

func scanNodeReport(rows *sql.Rows, runStatus storage.RunStatus) (storage.NodeReport, error) {
	var (
		report          storage.NodeReport
		reason          sql.NullString
		firstStartedAt  sql.NullTime
		lastStartedAt   sql.NullTime
		stateChangedAt  sql.NullTime
		finishedAt      sql.NullTime
		lastRunner      sql.NullString
		leaseOwner      sql.NullString
		leaseExpiresAt  sql.NullTime
		leaseGeneration int64
	)

	if err := rows.Scan(
		&report.RunID,
		&report.NodeID,
		&report.FunctionKey,
		&report.State,
		&reason,
		&report.Attempt,
		&report.MaxAttempts,
		&report.EligibleAt,
		&firstStartedAt,
		&lastStartedAt,
		&stateChangedAt,
		&finishedAt,
		&lastRunner,
		&leaseOwner,
		&leaseGeneration,
		&leaseExpiresAt,
	); err != nil {
		return storage.NodeReport{}, fmt.Errorf("scan node report: %w", err)
	}

	report.EligibleAt = report.EligibleAt.UTC()
	report.FirstStartedAt = utcTime(firstStartedAt)
	report.LastStartedAt = utcTime(lastStartedAt)
	report.StateChangedAt = utcTime(stateChangedAt)
	report.FinishedAt = utcTime(finishedAt)

	if lastRunner.Valid && lastRunner.String == "" {
		return storage.NodeReport{}, incompatible("latest runner is empty")
	}

	report.RunnerID = runnerID(lastRunner)
	if report.State == storage.NodeRunning && leaseOwner.Valid && leaseExpiresAt.Valid {
		report.CurrentLease = &storage.CurrentLease{
			ExpiresAt:  leaseExpiresAt.Time.UTC(),
			RunnerID:   storage.RunnerID(leaseOwner.String),
			Generation: leaseGeneration,
		}
	}

	validation := nodeValidation{
		runStatus:      runStatus,
		reason:         reason,
		lastRunner:     lastRunner,
		leaseOwner:     leaseOwner,
		leaseExpiresAt: leaseExpiresAt,
	}
	if err := validateNodeReport(&report, &validation); err != nil {
		return storage.NodeReport{}, fmt.Errorf("validate node %q: %w", report.NodeID, err)
	}

	return report, nil
}
