package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

const (
	defaultNodePageSize = 50
	maxNodePageSize     = 200

	nodePageQuery = `SELECT
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
)

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
	rows, err := transaction.QueryContext(
		ctx,
		nodePageQuery,
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
