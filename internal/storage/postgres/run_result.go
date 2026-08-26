package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// GetRunResult returns persisted run state and payloads.
func (s *Store) GetRunResult(
	ctx context.Context,
	runID storage.RunID,
) (storage.RunResult, error) {
	const query = `SELECT r.workflow_name, r.definition_hash, terminal.signature_hash,
		r.status, r.output_payload, r.error_payload, r.max_attempts,
		r.retry_base_delay_ns, r.retry_max_delay_ns, r.retry_policy_version
		FROM cord_runs AS r
		LEFT JOIN cord_nodes AS terminal
			ON terminal.run_id = r.id AND terminal.node_id = r.terminal_node_id
		WHERE r.id = $1`

	var (
		result                            storage.RunResult
		terminalSignature                 sql.NullString
		output, failure                   []byte
		retryBaseDelayNS, retryMaxDelayNS int64
	)

	err := s.pool.QueryRowContext(ctx, query, runID).Scan(
		&result.WorkflowName,
		&result.DefinitionHash,
		&terminalSignature,
		&result.Status,
		&output,
		&failure,
		&result.MaxAttempts,
		&retryBaseDelayNS,
		&retryMaxDelayNS,
		&result.RetryPolicyVersion,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return result, fmt.Errorf("read run result %q: %w", runID, storage.ErrRunNotFound)
	}

	if err != nil {
		return result, fmt.Errorf("read run result: %w", err)
	}

	if !terminalSignature.Valid {
		return result, fmt.Errorf(
			"read run result %q: terminal node is missing: %w",
			runID,
			storage.ErrRunIncompatible,
		)
	}

	result.TerminalSignatureHash = terminalSignature.String
	result.Output, result.Error = output, failure
	result.RetryBaseDelay = time.Duration(retryBaseDelayNS)
	result.RetryMaxDelay = time.Duration(retryMaxDelayNS)

	return result, nil
}
