package cord_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowGetReadsPreAsyncPersistedRunAfterMigration(t *testing.T) {
	t.Parallel()

	sourceDatabase := openSQLite(t)
	source, err := cord.New(t.Context(), sourceDatabase, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)

	flow := source.From("pre-async-public-get", addOne)
	runID, err := flow.Submit(t.Context(), 41)
	require.NoError(t, err)
	_, err = flow.Get(t.Context(), runID)
	require.NoError(t, err)

	var (
		definitionHash, terminalNodeID, functionKey, signatureHash string
		maxAttempts, retryPolicyVersion                            int
		retryBaseDelayNS, retryMaxDelayNS                          int64
	)

	err = sourceDatabase.QueryRowContext(t.Context(), `SELECT r.definition_hash, r.terminal_node_id,
		n.function_key, n.signature_hash, r.max_attempts, r.retry_base_delay_ns,
		r.retry_max_delay_ns, r.retry_policy_version FROM cord_runs r JOIN cord_nodes n
		ON n.run_id = r.id AND n.node_id = r.terminal_node_id WHERE r.id = ?`, runID).Scan(
		&definitionHash, &terminalNodeID, &functionKey, &signatureHash,
		&maxAttempts, &retryBaseDelayNS, &retryMaxDelayNS, &retryPolicyVersion,
	)
	require.NoError(t, err)
	require.NoError(t, source.Close())

	database := openSQLite(t)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	_, err = database.ExecContext(t.Context(), `DROP INDEX cord_runs_workflow_name_idempotency_key_idx`)
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), `ALTER TABLE cord_runs DROP COLUMN idempotency_key`)
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), `ALTER TABLE cord_runs DROP COLUMN submission_fingerprint`)
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), `DELETE FROM cord_schema_migrations WHERE version_id = 4`)
	require.NoError(t, err)

	persistedAt := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC).Format(time.RFC3339Nano)
	_, err = database.ExecContext(t.Context(), `INSERT INTO cord_runs (
		id, workflow_name, definition_hash, status, input_payload, output_payload,
		terminal_node_id, created_at, updated_at, completed_at, max_attempts,
		retry_base_delay_ns, retry_max_delay_ns, retry_policy_version
	) VALUES (?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		runID, "pre-async-public-get", definitionHash, []byte("41"), []byte("42"),
		terminalNodeID, persistedAt, persistedAt, persistedAt,
		maxAttempts, retryBaseDelayNS, retryMaxDelayNS, retryPolicyVersion)
	require.NoError(t, err)
	_, err = database.ExecContext(t.Context(), `INSERT INTO cord_nodes (
		run_id, node_id, function_key, signature_hash, status, remaining_deps,
		attempt, available_at, lease_generation, output_payload, started_at, completed_at
	) VALUES (?, ?, ?, ?, 'completed', 0, 1, ?, 1, ?, ?, ?)`,
		runID, terminalNodeID, functionKey, signatureHash, persistedAt, []byte("42"), persistedAt, persistedAt)
	require.NoError(t, err)

	runtime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	result, err := runtime.From("pre-async-public-get", addOne).Get(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}
