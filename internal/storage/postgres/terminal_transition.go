package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func (s *Store) terminal(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
	lease storage.Lease,
	payload storage.EncodedPayload,
	success bool,
	reason storage.TerminalReason,
) (bool, error) {
	retryCtx, cancel := leaseContext(ctx, lease.Remaining)
	defer cancel()

	accepted := false

	err := runTransaction(retryCtx, s.pool, "transition node", func(transaction *sql.Tx) error {
		accepted = false

		terminalNodeID, running, transitionErr := lockRunningRun(retryCtx, transaction, runID)
		if transitionErr != nil || !running {
			return transitionErr
		}

		// Capture the transition instant after acquiring the run lock. Using a
		// timestamp observed before a lock wait could accept a lease that expired
		// while this terminal transition was blocked.
		transitionedAt, timeErr := databaseInstant(retryCtx, transaction)
		if timeErr != nil {
			return timeErr
		}

		transitioned, transitionErr := transitionNode(retryCtx, transaction, &transitionNodeParams{
			runID:          runID,
			nodeID:         nodeID,
			lease:          lease,
			payload:        payload,
			success:        success,
			reason:         reason,
			transitionedAt: transitionedAt,
		})
		if transitionErr != nil || !transitioned {
			return transitionErr
		}

		if success {
			transitionErr = completeRunPath(retryCtx, transaction, &completeRunPathParams{
				runID:          runID,
				nodeID:         nodeID,
				payload:        payload,
				runnerID:       lease.Owner,
				terminal:       nodeID == terminalNodeID,
				transitionedAt: transitionedAt,
			})
		} else {
			transitionErr = failRunPath(
				retryCtx, transaction, runID, payload, lease.Owner, reason, transitionedAt,
			)
		}

		if transitionErr != nil {
			return transitionErr
		}

		accepted = true

		return nil
	})
	if err != nil {
		return false, err
	}

	return accepted, nil
}

func lockRunningRun(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
) (storage.NodeID, bool, error) {
	const query = `SELECT terminal_node_id FROM cord_runs
		WHERE id = $1 AND status = 'running'
		FOR UPDATE`

	var terminalNodeID storage.NodeID
	if err := transaction.QueryRowContext(ctx, query, runID).Scan(&terminalNodeID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}

		return "", false, fmt.Errorf("lock running run: %w", err)
	}

	return terminalNodeID, true, nil
}

type transitionNodeParams struct {
	runID          storage.RunID
	nodeID         storage.NodeID
	reason         storage.TerminalReason
	transitionedAt time.Time
	payload        storage.EncodedPayload
	lease          storage.Lease
	success        bool
}

func transitionNode(
	ctx context.Context,
	transaction *sql.Tx,
	params *transitionNodeParams,
) (bool, error) {
	const query = `UPDATE cord_nodes
		SET status = $1,
			output_payload = $2,
			error_payload = $3,
			lease_owner = NULL,
			lease_expires_at = NULL,
			completed_at = $8,
			state_changed_at = $8,
			terminal_reason = $9
				WHERE run_id = $4
			AND node_id = $5
			AND status = 'running'
			AND lease_owner = $6
			AND lease_generation = $7
			AND lease_expires_at > $8
			AND EXISTS (
				SELECT 1 FROM cord_runs WHERE id = $4 AND status = 'running'
			)`

	nodeStatus := storage.NodeFailed

	output, failure := any(nil), nullablePayload(params.payload)
	if params.success {
		nodeStatus, output, failure = storage.NodeCompleted, nullablePayload(params.payload), nil
	}

	if params.success {
		params.reason = storage.ReasonSucceeded
	}

	if !nodeStatus.AllowsReason(params.reason) {
		return false, fmt.Errorf(
			"transition node state: status %q does not allow terminal reason %q",
			nodeStatus,
			params.reason,
		)
	}

	result, err := transaction.ExecContext(
		ctx, query, nodeStatus, output, failure, params.runID, params.nodeID,
		params.lease.Owner, params.lease.Generation, params.transitionedAt, params.reason,
	)
	if err != nil {
		return false, fmt.Errorf("transition node state: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect node transition: %w", err)
	}

	return count == 1, nil
}
