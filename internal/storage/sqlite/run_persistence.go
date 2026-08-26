package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func requireForeignKeys(ctx context.Context, transaction *sql.Tx) error {
	var enabled bool
	if err := transaction.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&enabled); err != nil {
		return fmt.Errorf("inspect sqlite foreign-key enforcement: %w", err)
	}

	if !enabled {
		return errors.New("sqlite foreign-key enforcement is disabled; enable it for every connection")
	}

	return nil
}

func (s *Store) createRunContents(
	ctx context.Context,
	transaction *sql.Tx,
	plan *storage.RunPlan,
	createdAt time.Time,
) error {
	for index := range plan.Nodes {
		if err := insertNode(ctx, transaction, plan.Run.ID, &plan.Nodes[index], createdAt); err != nil {
			return err
		}
	}

	for _, edge := range plan.Edges {
		if err := insertEdge(ctx, transaction, plan.Run.ID, edge); err != nil {
			return err
		}
	}

	return nil
}

func attachCompatibleRun(
	ctx context.Context,
	transaction *sql.Tx,
	plan *storage.RunPlan,
	insertErr error,
) (storage.RunID, error) {
	if plan.Run.IdempotencyKey == nil {
		return "", insertErr
	}

	var (
		existingID            storage.RunID
		definitionHash        string
		submissionFingerprint sql.NullString
	)

	err := transaction.QueryRowContext(ctx, `SELECT id, definition_hash, submission_fingerprint
		FROM cord_runs WHERE workflow_name = ? AND idempotency_key = ?`,
		plan.Run.WorkflowName, *plan.Run.IdempotencyKey,
	).Scan(&existingID, &definitionHash, &submissionFingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return "", insertErr
	}

	if err != nil {
		return "", fmt.Errorf("inspect idempotent run attachment: %w", err)
	}

	if definitionHash != plan.Run.DefinitionHash || !submissionFingerprint.Valid ||
		plan.Run.SubmissionFingerprint == nil || submissionFingerprint.String != *plan.Run.SubmissionFingerprint {
		return "", fmt.Errorf(
			"attach run for workflow %q and idempotency key: %w",
			plan.Run.WorkflowName,
			storage.ErrRunConflict,
		)
	}

	return existingID, nil
}

func insertRun(
	ctx context.Context,
	transaction *sql.Tx,
	run *storage.Run,
	createdAt time.Time,
) error {
	_, err := transaction.ExecContext(
		ctx,
		insertRunStatement,
		run.ID,
		run.WorkflowName,
		run.DefinitionHash,
		run.Status,
		[]byte(run.Input),
		nullPayload(run.Output),
		run.TerminalNodeID,
		nullPayload(run.Error),
		formatTime(createdAt),
		formatTime(createdAt),
		nullTimePointer(run.CompletedAt),
		run.MaxAttempts,
		run.RetryBaseDelay.Nanoseconds(),
		run.RetryMaxDelay.Nanoseconds(),
		run.RetryPolicyVersion,
		nullStringPointer(run.IdempotencyKey),
		nullStringPointer(run.SubmissionFingerprint),
	)
	if err != nil {
		return fmt.Errorf("insert run %q: %w", run.ID, err)
	}

	return nil
}

func insertNode(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	node *storage.Node,
	createdAt time.Time,
) error {
	_, err := transaction.ExecContext(
		ctx,
		insertNodeStatement,
		runID,
		node.ID,
		node.FunctionKey,
		node.SignatureHash,
		node.Status,
		node.RemainingDeps,
		node.Attempt,
		formatTime(node.AvailableAt),
		nullString(node.Lease.Owner),
		node.Lease.Generation,
		nullTime(node.Lease.ExpiresAt),
		nullPayload(node.Output),
		nullPayload(node.Error),
		nullTimePointer(node.StartedAt),
		nullTimePointer(node.CompletedAt),
		formatTime(createdAt),
	)
	if err != nil {
		return fmt.Errorf("insert node %q for run %q: %w", node.ID, runID, err)
	}

	return nil
}

func insertEdge(
	ctx context.Context,
	transaction *sql.Tx,
	runID storage.RunID,
	edge storage.Edge,
) error {
	_, err := transaction.ExecContext(
		ctx,
		insertEdgeStatement,
		runID,
		edge.Parent,
		edge.Child,
		edge.ParentOrder,
	)
	if err != nil {
		return fmt.Errorf(
			"insert edge %q -> %q for run %q: %w",
			edge.Parent,
			edge.Child,
			runID,
			err,
		)
	}

	return nil
}
