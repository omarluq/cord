package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStore_CreateRun(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	now := time.Now().UTC().Truncate(time.Millisecond)
	plan := validPlan(now, "run-1")

	requireCreateRun(t.Context(), t, store, &plan)

	assertRowCounts(t, database, map[string]int{
		edgesTable: 1,
		nodesTable: 2,
		runsTable:  1,
	})

	var (
		workflowName   string
		definitionHash string
		status         string
		input          []byte
		output         []byte
		errorPayload   []byte
	)

	err := database.QueryRowContext(
		t.Context(),
		`SELECT workflow_name, definition_hash, status, input_payload, output_payload, error_payload
		 FROM cord_runs WHERE id = ?`,
		plan.Run.ID,
	).Scan(&workflowName, &definitionHash, &status, &input, &output, &errorPayload)
	require.NoError(t, err)
	assert.Equal(t, plan.Run.WorkflowName, workflowName)
	assert.Equal(t, plan.Run.DefinitionHash, definitionHash)
	assert.Equal(t, string(plan.Run.Status), status)
	assert.Equal(t, []byte(plan.Run.Input), input)
	assert.Nil(t, output)
	assert.Nil(t, errorPayload)
}

func TestStore_CreateRunParameterizesAllValues(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	injection := `'); DROP TABLE cord_runs; --`
	plan := validPlan(time.Now().UTC(), storage.RunID(injection))
	plan.Run.WorkflowName = injection
	plan.Nodes[0].ID = storage.NodeID(injection)
	plan.Edges[0].Parent = storage.NodeID(injection)

	requireCreateRun(t.Context(), t, store, &plan)

	var workflowName string

	err := database.QueryRowContext(
		t.Context(),
		"SELECT workflow_name FROM cord_runs WHERE id = ?",
		injection,
	).Scan(&workflowName)
	require.NoError(t, err)
	assert.Equal(t, injection, workflowName)
	assertRowCounts(t, database, map[string]int{
		edgesTable: 1,
		nodesTable: 2,
		runsTable:  1,
	})
}

func TestStore_CreateRunRejectsDisabledSQLiteForeignKeys(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, false)
	plan := validPlan(time.Now().UTC(), "foreign-keys-disabled")

	_, _, err := store.CreateOrAttachRun(t.Context(), &plan)

	require.ErrorContains(t, err, "sqlite foreign-key enforcement is disabled")
	assertRowCounts(t, database, map[string]int{runsTable: 0})
}

func TestStore_SQLiteForeignKeysCascadeRunDeletion(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "cascade-run")
	requireCreateRun(t.Context(), t, store, &plan)

	_, err := database.ExecContext(t.Context(), "DELETE FROM cord_runs WHERE id = ?", plan.Run.ID)
	require.NoError(t, err)

	assertRowCounts(t, database, map[string]int{
		edgesTable: 0,
		nodesTable: 0,
		runsTable:  0,
	})
}

func TestStore_CreateRunRollsBackIncompletePlan(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "run-rollback")
	plan.Nodes[1].RemainingDeps = 2
	plan.Edges = append(plan.Edges, plan.Edges[0])

	requireCreateRunError(t.Context(), t, store, &plan)
	assertRowCounts(t, database, map[string]int{
		edgesTable: 0,
		nodesTable: 0,
		runsTable:  0,
	})
}
