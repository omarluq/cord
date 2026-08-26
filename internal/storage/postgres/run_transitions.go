package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

type completeRunPathParams struct {
	runID          storage.RunID
	nodeID         storage.NodeID
	runnerID       string
	transitionedAt time.Time
	payload        storage.EncodedPayload
	terminal       bool
}

func completeRunPath(
	ctx context.Context,
	transaction *sql.Tx,
	params *completeRunPathParams,
) error {
	const releaseChildren = `UPDATE cord_nodes
		SET remaining_deps = remaining_deps - 1,
			status = CASE WHEN remaining_deps = 1 THEN 'ready' ELSE status END,
			available_at = CASE
				WHEN remaining_deps = 1 THEN $3 ELSE available_at
			END,
			state_changed_at = CASE
				WHEN remaining_deps = 1
					 THEN $3
				ELSE state_changed_at
			END
				WHERE run_id = $1
			AND status = 'pending'
			AND remaining_deps > 0
			AND node_id IN (
				SELECT child_node_id FROM cord_edges
				WHERE run_id = $1 AND parent_node_id = $2
			)`
	if _, err := transaction.ExecContext(
		ctx, releaseChildren, params.runID, params.nodeID, params.transitionedAt,
	); err != nil {
		return fmt.Errorf("release child nodes: %w", err)
	}

	if !params.terminal {
		return nil
	}

	const completeRun = `UPDATE cord_runs
		SET status = $1,
			output_payload = $2,
			error_payload = NULL,
			updated_at = $5,
			completed_at = $5,
			terminal_reason = $6,
			terminal_runner_id = $7
				WHERE id = $3 AND status = 'running' AND terminal_node_id = $4`

	result, err := transaction.ExecContext(
		ctx, completeRun, storage.RunCompleted, nullablePayload(params.payload),
		params.runID, params.nodeID, params.transitionedAt, storage.ReasonSucceeded, params.runnerID,
	)
	if err != nil {
		return fmt.Errorf("complete run: %w", err)
	}

	return requireOneAffected(result, "run completion")
}

func failRunPath(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	payload storage.EncodedPayload,
	runnerID string,
	reason storage.TerminalReason,
	transitionedAt time.Time,
) error {
	if err := cancelUnfinishedNodes(
		ctx, transaction, runID, storage.ReasonCanceledByRunFailure, transitionedAt,
	); err != nil {
		return err
	}

	const failRun = `UPDATE cord_runs
		SET status = $1,
			output_payload = NULL,
			error_payload = $2,
			updated_at = $4,
			completed_at = $4,
			terminal_reason = $5,
			terminal_runner_id = $6
				WHERE id = $3 AND status = 'running'`

	result, err := transaction.ExecContext(
		ctx, failRun, storage.RunFailed, nullablePayload(payload), runID,
		transitionedAt, reason, runnerID,
	)
	if err != nil {
		return fmt.Errorf("fail run: %w", err)
	}

	return requireOneAffected(result, "run failure")
}

func cancelUnfinishedNodes(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	reason storage.TerminalReason,
	transitionedAt time.Time,
) error {
	const query = `UPDATE cord_nodes
		SET status = $1,
			lease_owner = NULL,
			lease_expires_at = NULL,
			completed_at = $7,
			state_changed_at = $7,
			terminal_reason = $8
				WHERE run_id = $2 AND status IN ($3, $4, $5, $6)`
	if _, err := transaction.ExecContext(
		ctx,
		query,
		storage.NodeCanceled,
		runID,
		storage.NodePending,
		storage.NodeReady,
		storage.NodeRunning,
		storage.NodeRetryWait,
		transitionedAt,
		reason,
	); err != nil {
		return fmt.Errorf("cancel unfinished nodes for run %q: %w", runID, err)
	}

	return nil
}

func requireOneAffected(result sql.Result, operation string) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect %s: %w", operation, err)
	}

	if count != 1 {
		return fmt.Errorf("%s affected %d rows, expected 1", operation, count)
	}

	return nil
}
