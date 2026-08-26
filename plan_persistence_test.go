package cord_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflow_RunPersistsReachablePlan(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime := newRuntimeForDB(t, database)

	root := runtime.From("test-workflow", increment)
	selected := root.Then(double)
	_ = root.Then(increment)

	result, err := selected.Run(t.Context(), 3)
	require.NoError(t, err)
	assert.Equal(t, 8, result)

	var (
		runID           string
		workflowName    string
		definitionHash  string
		status          string
		input           []byte
		rootFunctionKey string
		nodeCount       int
		edgeCount       int
	)

	err = database.QueryRowContext(
		t.Context(),
		"SELECT id, workflow_name, definition_hash, status, input_payload FROM cord_runs",
	).Scan(&runID, &workflowName, &definitionHash, &status, &input)
	require.NoError(t, err)
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		"SELECT function_key FROM cord_nodes WHERE remaining_deps = 0",
	).Scan(&rootFunctionKey))
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_nodes").Scan(&nodeCount))
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_edges").Scan(&edgeCount))

	identifier, parseErr := uuid.FromString(runID)
	require.NoError(t, parseErr)
	assert.Equal(t, uuid.V7, identifier.Version())
	assert.Equal(t, "test-workflow", workflowName)
	assert.NotEqual(t, rootFunctionKey, workflowName)
	assert.Len(t, definitionHash, 64)
	assert.Equal(t, "completed", status)
	assert.JSONEq(t, "3", string(input))
	assert.Equal(t, 2, nodeCount)
	assert.Equal(t, 1, edgeCount)
}

func TestWorkflow_RunRejectsClosureBeforeInsertion(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime := newRuntimeForDB(t, database)

	called := false
	workflow := runtime.From("test-workflow", func(_ context.Context, value int) (int, error) {
		called = true

		return value, nil
	})

	_, err := workflow.Run(t.Context(), 1)
	require.ErrorContains(t, err, "not a named package-level function")
	assert.False(t, called)

	var count int
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_runs").Scan(&count))
	assert.Zero(t, count)
}

func sqliteTableExists(t *testing.T, database *sql.DB, table string) bool {
	t.Helper()

	var name string

	err := database.QueryRowContext(
		t.Context(),
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&name)

	return err == nil && name == table
}
