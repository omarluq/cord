package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const (
	defaultNodePageSize = 50
	maxNodePageSize     = 200
)

// InspectRun returns one consistent, payload-free snapshot of a run and its
// node-state counts.
func (s *Store) InspectRun(ctx context.Context, runID storage.RunID) (storage.RunReport, error) {
	const query = `SELECT
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

	var (
		report         storage.RunReport
		reason         sql.NullString
		firstStartedAt sql.NullTime
		finishedAt     sql.NullTime
		terminalRunner sql.NullString
		totalNodes     int
	)

	err := s.pool.QueryRowContext(ctx, query, runID).Scan(
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

// ListRunNodes returns a bounded keyset page ordered by node ID. The storage
// continuation token is the last node ID; the public API wraps it opaquely.
func (s *Store) ListRunNodes(
	ctx context.Context,
	runID storage.RunID,
	query storage.NodeQuery,
) (_ storage.NodePage, err error) {
	limit, err := normalizeNodeQuery(query)
	if err != nil {
		return storage.NodePage{}, fmt.Errorf("list nodes for run %q: %w", runID, err)
	}

	transaction, err := s.pool.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return storage.NodePage{}, fmt.Errorf("list nodes for run %q: begin read: %w", runID, err)
	}
	defer func() {
		if rollbackErr := transaction.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("roll back read: %w", rollbackErr))
		}
	}()

	page, readErr := listRunNodes(ctx, transaction, runID, query, limit)
	if readErr != nil {
		return storage.NodePage{}, readErr
	}

	if err = transaction.Commit(); err != nil {
		return storage.NodePage{}, fmt.Errorf("list nodes for run %q: commit read: %w", runID, err)
	}

	return page, nil
}

func listRunNodes(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	query storage.NodeQuery,
	limit int,
) (storage.NodePage, error) {
	runStatus, err := readNodePageRun(ctx, transaction, runID)
	if err != nil {
		return storage.NodePage{}, err
	}

	return queryNodePage(ctx, transaction, runID, runStatus, query, limit)
}

func readNodePageRun(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
) (storage.RunStatus, error) {
	const query = `SELECT status, max_attempts FROM cord_runs WHERE id = $1`

	var (
		status      storage.RunStatus
		maxAttempts int
	)
	if err := transaction.QueryRowContext(ctx, query, runID).Scan(&status, &maxAttempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("list nodes for run %q: %w", runID, storage.ErrRunNotFound)
		}

		return "", fmt.Errorf("list nodes for run %q: read run: %w", runID, err)
	}

	if !status.IsKnown() || maxAttempts < 1 {
		return "", incompatible("invalid run metadata for node page")
	}

	return status, nil
}

func queryNodePage(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	runStatus storage.RunStatus,
	query storage.NodeQuery,
	limit int,
) (_ storage.NodePage, err error) {
	const statement = `SELECT
		n.run_id, n.node_id, n.function_key, n.status, n.terminal_reason,
		n.attempt, r.max_attempts, n.available_at, n.started_at, n.last_started_at,
		n.state_changed_at, n.completed_at, n.last_runner_id,
		n.lease_owner, n.lease_generation, n.lease_expires_at
	FROM cord_nodes n
	JOIN cord_runs r ON r.id = n.run_id
	WHERE n.run_id = $1
		AND n.node_id > $2
		AND ($3::text IS NULL OR n.status = $3)
		AND ($4::text IS NULL OR n.terminal_reason = $4)
	ORDER BY n.node_id
	LIMIT $5`

	rows, err := transaction.QueryContext(
		ctx,
		statement,
		runID,
		query.ContinuationToken,
		nodeStateFilter(query.State),
		nodeReasonFilter(query.Reason),
		limit+1,
	)
	if err != nil {
		return storage.NodePage{}, fmt.Errorf("list nodes for run %q: query page: %w", runID, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("list nodes for run %q: close page: %w", runID, closeErr))
		}
	}()

	page := storage.NodePage{
		Nodes:             make([]storage.NodeReport, 0, limit),
		ContinuationToken: "",
	}

	for rows.Next() {
		node, scanErr := scanNodeReport(rows, runStatus)
		if scanErr != nil {
			return storage.NodePage{}, fmt.Errorf("list nodes for run %q: %w", runID, scanErr)
		}

		page.Nodes = append(page.Nodes, node)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return storage.NodePage{}, fmt.Errorf("list nodes for run %q: read page: %w", runID, rowsErr)
	}

	if len(page.Nodes) > limit {
		page.ContinuationToken = string(page.Nodes[limit-1].NodeID)
		page.Nodes = page.Nodes[:limit]
	}

	return page, nil
}

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

	if err := validateVersionedRunReason(report, reason, terminal); err != nil {
		return err
	}

	return validateVersionedRunTimestamps(report, terminal)
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

func validateVersionedRunReason(report *storage.RunReport, reason sql.NullString, terminal bool) error {
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

func validateVersionedRunTimestamps(report *storage.RunReport, terminal bool) error {
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

	if err := validateVersionedNodeReason(report, validation.reason, terminal); err != nil {
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

func validateVersionedNodeReason(
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

func normalizeNodeQuery(query storage.NodeQuery) (int, error) {
	if err := validateNodeFilters(query); err != nil {
		return 0, err
	}

	if query.PageSize < 0 || query.PageSize > maxNodePageSize {
		return 0, fmt.Errorf("page size must be between 0 and %d", maxNodePageSize)
	}

	if query.PageSize == 0 {
		return defaultNodePageSize, nil
	}

	return query.PageSize, nil
}

func validateNodeFilters(query storage.NodeQuery) error {
	if query.State != nil && !query.State.IsKnown() {
		return fmt.Errorf("unknown node state filter %q", *query.State)
	}

	if query.Reason != nil && !query.Reason.IsKnown() {
		return fmt.Errorf("unknown node reason filter %q", *query.Reason)
	}

	if query.State != nil && query.Reason != nil && !query.State.AllowsReason(*query.Reason) {
		return fmt.Errorf("node state %q does not allow reason %q", *query.State, *query.Reason)
	}

	return nil
}

func nodeStateFilter(state *storage.NodeStatus) any {
	if state == nil {
		return nil
	}

	return *state
}

func nodeReasonFilter(reason *storage.TerminalReason) any {
	if reason == nil {
		return nil
	}

	return *reason
}

func utcTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}

	instant := value.Time.UTC()

	return &instant
}

func runnerID(value sql.NullString) *storage.RunnerID {
	if !value.Valid || value.String == "" {
		return nil
	}

	id := storage.RunnerID(value.String)

	return &id
}

func sumNodeCounts(counts storage.NodeStateCounts) int {
	return counts.Pending + counts.Ready + counts.Running + counts.RetryWait +
		counts.Completed + counts.Failed + counts.Canceled
}

func incompatible(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", storage.ErrRunIncompatible, fmt.Sprintf(format, arguments...))
}
