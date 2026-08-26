package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

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
		maxAttempts int
	)

	scanErr := transaction.QueryRowContext(ctx,
		"SELECT status, max_attempts FROM cord_runs WHERE id = ?", runID,
	).Scan(&runStatus, &maxAttempts)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return "", 0, fmt.Errorf("list nodes for run %q: %w", runID, storage.ErrRunNotFound)
	}

	if scanErr != nil {
		return "", 0, fmt.Errorf("inspect node-page run %q: %w", runID, scanErr)
	}

	if !runStatus.IsKnown() || maxAttempts < 1 {
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
	_ storage.RunStatus,
	query storage.NodeQuery,
	limit int,
) (statement string, arguments []any) {
	statement = `SELECT n.node_id, n.function_key, n.status, n.attempt,
		n.available_at, n.started_at, n.last_started_at, n.state_changed_at, n.completed_at,
		n.terminal_reason, n.last_runner_id,
		n.lease_owner, n.lease_generation, n.lease_expires_at
	FROM cord_nodes AS n
	WHERE n.run_id = ? AND n.node_id > ?`
	arguments = []any{runID, query.ContinuationToken}

	if query.State != nil {
		statement += " AND n.status = ?"

		arguments = append(arguments, *query.State)
	}

	if query.Reason != nil {
		statement += " AND n.terminal_reason = ?"

		arguments = append(arguments, *query.Reason)
	}

	return statement + " ORDER BY n.node_id LIMIT ?", append(arguments, limit+1)
}
