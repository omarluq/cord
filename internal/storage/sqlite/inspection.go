package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// InspectRun reads one consistent, payload-free snapshot of a run and its node counts.
func (s *Store) InspectRun(ctx context.Context, runID storage.RunID) (storage.RunReport, error) {
	var (
		report                           storage.RunReport
		createdAt, updatedAt             string
		startedAt, completedAt           sql.NullString
		lifecycleVersion                 sql.NullInt64
		terminalReason, terminalRunnerID sql.NullString
		totalNodes, knownNodes           int
	)

	err := s.database.QueryRowContext(ctx, `SELECT
		r.id, r.workflow_name, r.status, r.created_at, r.updated_at,
		r.started_at, r.completed_at, r.lifecycle_version,
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
		&startedAt, &completedAt, &lifecycleVersion, &terminalReason, &terminalRunnerID,
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

	if err = validateRunReport(&report, lifecycleVersion, terminalReason); err != nil {
		return storage.RunReport{}, incompatibleRun(runID, "%v", err)
	}

	return report, nil
}

// ListRunNodes reads one bounded, payload-free keyset page ordered by node ID.
func (s *Store) ListRunNodes(
	ctx context.Context,
	runID storage.RunID,
	query storage.NodeQuery,
) (_ storage.NodePage, err error) {
	limit, err := normalizeNodeQuery(query)
	if err != nil {
		return storage.NodePage{}, err
	}

	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return storage.NodePage{}, fmt.Errorf("begin node inspection: %w", err)
	}
	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("finish node inspection: %w", rollbackErr))
		}
	}()

	runStatus, maxAttempts, err := inspectNodePageRun(ctx, transaction, runID)
	if err != nil {
		return storage.NodePage{}, err
	}

	statement, arguments := nodePageQuery(runID, runStatus, query, limit)

	rows, err := transaction.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return storage.NodePage{}, fmt.Errorf("list nodes for run %q: %w", runID, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close node page: %w", closeErr))
		}
	}()

	page, err := scanNodePage(rows, runID, runStatus, maxAttempts, limit)
	if err != nil {
		return storage.NodePage{}, err
	}

	if commitErr := transaction.Commit(); commitErr != nil {
		return storage.NodePage{}, fmt.Errorf("commit node inspection: %w", commitErr)
	}

	return page, nil
}

func inspectNodePageRun(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
) (storage.RunStatus, int, error) {
	var (
		runStatus   storage.RunStatus
		runVersion  sql.NullInt64
		maxAttempts int
	)

	scanErr := transaction.QueryRowContext(ctx,
		"SELECT status, lifecycle_version, max_attempts FROM cord_runs WHERE id = ?", runID,
	).Scan(&runStatus, &runVersion, &maxAttempts)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return "", 0, fmt.Errorf("list nodes for run %q: %w", runID, storage.ErrRunNotFound)
	}

	if scanErr != nil {
		return "", 0, fmt.Errorf("inspect node-page run %q: %w", runID, scanErr)
	}

	validVersion := !runVersion.Valid || storage.LifecycleVersion(runVersion.Int64) == storage.LifecycleVersion1
	if !runStatus.IsKnown() || maxAttempts < 1 || !validVersion {
		return "", 0, incompatibleRun(runID, "invalid run metadata for node page")
	}

	return runStatus, maxAttempts, nil
}

func scanNodePage(
	rows *sql.Rows,
	runID storage.RunID,
	runStatus storage.RunStatus,
	maxAttempts, limit int,
) (storage.NodePage, error) {
	page := storage.NodePage{
		ContinuationToken: "",
		Nodes:             make([]storage.NodeReport, 0, limit),
	}

	for rows.Next() {
		report, err := scanNodeReport(rows, runID, runStatus, maxAttempts)
		if err != nil {
			return storage.NodePage{}, err
		}

		page.Nodes = append(page.Nodes, report)
	}

	if err := rows.Err(); err != nil {
		return storage.NodePage{}, fmt.Errorf("iterate nodes for run %q: %w", runID, err)
	}

	if len(page.Nodes) > limit {
		page.ContinuationToken = string(page.Nodes[limit-1].NodeID)
		page.Nodes = page.Nodes[:limit]
	}

	return page, nil
}

func normalizeNodeQuery(query storage.NodeQuery) (int, error) {
	if err := validateNodeQuery(query); err != nil {
		return 0, err
	}

	if query.PageSize == 0 {
		return storage.DefaultNodePageSize, nil
	}

	return query.PageSize, nil
}

func validateNodeQuery(query storage.NodeQuery) error {
	if query.PageSize < 0 || query.PageSize > storage.MaxNodePageSize {
		return fmt.Errorf("list run nodes: page size must be between 0 and %d", storage.MaxNodePageSize)
	}

	if query.State != nil && !query.State.IsKnown() {
		return fmt.Errorf("list run nodes: unknown state %q", *query.State)
	}

	if query.Reason != nil && !query.Reason.IsKnown() {
		return fmt.Errorf("list run nodes: unknown reason %q", *query.Reason)
	}

	if query.State != nil && query.Reason != nil && !query.State.AllowsReason(*query.Reason) {
		return fmt.Errorf("list run nodes: reason %q is invalid for state %q", *query.Reason, *query.State)
	}

	return nil
}

func nodePageQuery(
	runID storage.RunID,
	runStatus storage.RunStatus,
	query storage.NodeQuery,
	limit int,
) (statement string, arguments []any) {
	statement = `SELECT n.node_id, n.function_key, n.status, n.attempt,
		n.available_at, n.started_at, n.last_started_at, n.state_changed_at, n.completed_at,
		n.lifecycle_version, n.terminal_reason, n.last_runner_id,
		n.lease_owner, n.lease_generation, n.lease_expires_at
	FROM cord_nodes AS n
	WHERE n.run_id = ? AND n.node_id > ?`
	arguments = []any{runID, query.ContinuationToken}

	if query.State != nil {
		statement += " AND n.status = ?"

		arguments = append(arguments, *query.State)
	}

	if query.Reason != nil {
		statement += ` AND CASE
			WHEN n.lifecycle_version IS NULL AND n.status = ? THEN ?
			WHEN n.lifecycle_version IS NULL AND n.status = ? THEN ?
			WHEN n.lifecycle_version IS NULL AND n.status = ? AND ? = ? THEN ?
			WHEN n.lifecycle_version IS NULL AND n.status = ? THEN ?
			ELSE COALESCE(n.terminal_reason, '') END = ?`

		arguments = append(arguments,
			storage.NodeCompleted, storage.ReasonSucceeded,
			storage.NodeFailed, storage.ReasonLegacyUnknown,
			storage.NodeCanceled, runStatus, storage.RunFailed, storage.ReasonCanceledByRunFailure,
			storage.NodeCanceled, storage.ReasonLegacyUnknown,
			*query.Reason,
		)
	}

	return statement + " ORDER BY n.node_id LIMIT ?", append(arguments, limit+1)
}

type rowScanner interface {
	Scan(...any) error
}

type nodeReportValidation struct {
	runStatus        storage.RunStatus
	terminalReason   sql.NullString
	lastRunnerID     sql.NullString
	leaseOwner       sql.NullString
	leaseExpiresAt   sql.NullString
	lifecycleVersion sql.NullInt64
	leaseGeneration  int64
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
		lifecycleVersion                                         sql.NullInt64
		terminalReason, lastRunnerID, leaseOwner, leaseExpiresAt sql.NullString
		leaseGeneration                                          int64
	)

	report.RunID = runID
	report.MaxAttempts = maxAttempts

	if err := rows.Scan(
		&report.NodeID, &report.FunctionKey, &report.State, &report.Attempt,
		&availableAt, &startedAt, &lastStartedAt, &stateChangedAt, &completedAt,
		&lifecycleVersion, &terminalReason, &lastRunnerID,
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

	if err := populateCurrentLease(
		&report, lifecycleVersion, leaseOwner, leaseExpiresAt, leaseGeneration,
	); err != nil {
		return report, incompatibleNode(runID, report.NodeID, "%v", err)
	}

	validation := nodeReportValidation{
		runStatus:        runStatus,
		lifecycleVersion: lifecycleVersion,
		terminalReason:   terminalReason,
		lastRunnerID:     lastRunnerID,
		leaseOwner:       leaseOwner,
		leaseExpiresAt:   leaseExpiresAt,
		leaseGeneration:  leaseGeneration,
	}
	if err := validateNodeReport(&report, &validation); err != nil {
		return storage.NodeReport{}, incompatibleNode(runID, report.NodeID, "%v", err)
	}

	return report, nil
}

func validateRunReport(
	report *storage.RunReport,
	version sql.NullInt64,
	reason sql.NullString,
) error {
	terminal, err := validateRunBasics(report)
	if err != nil {
		return err
	}

	if !version.Valid {
		return validateLegacyRunReport(report, reason)
	}

	return validateCurrentRunReport(report, version.Int64, reason, terminal)
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

func validateLegacyRunReport(report *storage.RunReport, reason sql.NullString) error {
	if reason.Valid || report.TerminalRunnerID != nil || report.FirstStartedAt != nil {
		return errors.New("legacy run contains lifecycle metadata")
	}

	if mapped, ok := report.State.LegacyReason(); ok {
		report.Reason = mapped
	}

	return nil
}

func validateCurrentRunReport(
	report *storage.RunReport,
	version int64,
	reason sql.NullString,
	terminal bool,
) error {
	if storage.LifecycleVersion(version) != storage.LifecycleVersion1 {
		return fmt.Errorf("unsupported lifecycle version %d", version)
	}

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
	if reason.Valid {
		report.Reason = storage.TerminalReason(reason.String)
	}

	if terminal != reason.Valid {
		return errors.New("terminal state and reason disagree")
	}

	if !report.State.AllowsReason(report.Reason) || (report.Reason != "" && !report.Reason.IsKnown()) {
		return fmt.Errorf("reason %q is invalid for state %q", report.Reason, report.State)
	}

	return nil
}

func validateNodeReport(report *storage.NodeReport, validation *nodeReportValidation) error {
	terminal, err := validateNodeBasics(
		report, validation.leaseOwner, validation.leaseExpiresAt, validation.leaseGeneration,
	)
	if err != nil {
		return err
	}

	if !validation.lifecycleVersion.Valid {
		return validateLegacyNodeReport(
			report, validation.runStatus, validation.terminalReason, validation.lastRunnerID,
		)
	}

	return validateCurrentNodeReport(
		report, validation.lifecycleVersion.Int64, validation.terminalReason, terminal,
	)
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

func validateLegacyNodeReport(
	report *storage.NodeReport,
	runStatus storage.RunStatus,
	reason, lastRunnerID sql.NullString,
) error {
	if reason.Valid || report.StateChangedAt != nil || report.LastStartedAt != nil || lastRunnerID.Valid {
		return errors.New("legacy node contains lifecycle metadata")
	}

	if mapped, ok := report.State.LegacyReason(runStatus); ok {
		report.Reason = mapped
	}

	return nil
}

func validateCurrentNodeReport(
	report *storage.NodeReport,
	version int64,
	reason sql.NullString,
	terminal bool,
) error {
	if storage.LifecycleVersion(version) != storage.LifecycleVersion1 {
		return fmt.Errorf("unsupported lifecycle version %d", version)
	}

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
	if reason.Valid {
		report.Reason = storage.TerminalReason(reason.String)
	}

	if terminal != reason.Valid {
		return errors.New("terminal state and reason disagree")
	}

	if !report.State.AllowsReason(report.Reason) || (report.Reason != "" && !report.Reason.IsKnown()) {
		return fmt.Errorf("reason %q is invalid for state %q", report.Reason, report.State)
	}

	return nil
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
	lifecycleVersion sql.NullInt64,
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
	if report.RunnerID == nil && !lifecycleVersion.Valid {
		runnerID := storage.RunnerID(leaseOwner.String)
		report.RunnerID = &runnerID
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
