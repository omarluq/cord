package storage_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// Register the sqlite driver used by openDatabase.
	_ "modernc.org/sqlite"
)

const (
	edgesTable = "cord_edges"
	nodesTable = "cord_nodes"
	runsTable  = "cord_runs"
)

func TestNewStore_RejectsNilDatabase(t *testing.T) {
	t.Parallel()

	store, err := storage.NewStore(nil)

	assert.Nil(t, store)
	require.Error(t, err)
}

func TestStore_CreateRun(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	now := time.Now().UTC().Truncate(time.Millisecond)
	plan := validPlan(now, "run-1")

	require.NoError(t, store.CreateRun(t.Context(), &plan))

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

	require.NoError(t, store.CreateRun(t.Context(), &plan))

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

	err := store.CreateRun(t.Context(), &plan)

	require.ErrorContains(t, err, "sqlite foreign-key enforcement is disabled")
	assertRowCounts(t, database, map[string]int{runsTable: 0})
}

func TestStore_SQLiteForeignKeysCascadeRunDeletion(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "cascade-run")
	require.NoError(t, store.CreateRun(t.Context(), &plan))

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

	require.Error(t, store.CreateRun(t.Context(), &plan))
	assertRowCounts(t, database, map[string]int{
		edgesTable: 0,
		nodesTable: 0,
		runsTable:  0,
	})
}

func TestStore_CreateRunRejectsInvalidPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*storage.RunPlan)
		name   string
	}{
		{name: "nil plan", mutate: nil},
		{
			name: "mismatched node run",
			mutate: func(plan *storage.RunPlan) {
				plan.Nodes[0].RunID = "another-run"
			},
		},
		{
			name: "mismatched edge run",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges[0].RunID = "another-run"
			},
		},
		{
			name: "missing terminal node",
			mutate: func(plan *storage.RunPlan) {
				plan.Run.TerminalNodeID = "missing"
			},
		},
		{
			name: "missing edge endpoint",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges[0].Parent = "missing"
			},
		},
		{
			name: "missing edge child",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges[0].Child = "absent"
			},
		},
		{
			name: "incorrect dependency count",
			mutate: func(plan *storage.RunPlan) {
				plan.Nodes[1].RemainingDeps = 0
			},
		},
		{
			name: "incorrect initial status",
			mutate: func(plan *storage.RunPlan) {
				plan.Nodes[0].Status = storage.NodePending
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)

			var plan *storage.RunPlan

			if testCase.mutate != nil {
				candidate := validPlan(time.Now().UTC(), "invalid-run")
				testCase.mutate(&candidate)
				plan = &candidate
			}

			require.Error(t, store.CreateRun(t.Context(), plan))
			assert.Equal(t, 0, rowCount(t, database, runsTable))
		})
	}
}

func TestStore_CreateRunRejectsCycles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*storage.RunPlan)
		name   string
	}{
		{
			name: "two nodes",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges = append(plan.Edges, storage.Edge{
					RunID:       plan.Run.ID,
					Parent:      plan.Nodes[1].ID,
					Child:       plan.Nodes[0].ID,
					ParentOrder: 0,
				})
				plan.Nodes[0].RemainingDeps = 1
			},
		},
		{
			name: "self cycle",
			mutate: func(plan *storage.RunPlan) {
				plan.Edges = append(plan.Edges, storage.Edge{
					RunID:       plan.Run.ID,
					Parent:      plan.Nodes[0].ID,
					Child:       plan.Nodes[0].ID,
					ParentOrder: 0,
				})
				plan.Nodes[0].RemainingDeps = 1
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, store := newStore(t, true)
			plan := validPlan(time.Now().UTC(), "cycle-run")
			testCase.mutate(&plan)

			require.ErrorContains(t, store.CreateRun(t.Context(), &plan), "cycle")
			assert.Equal(t, 0, rowCount(t, database, runsTable))
		})
	}
}

func TestStore_CreateRunDuplicatePreservesOriginal(t *testing.T) {
	t.Parallel()

	database, store := newStore(t, true)
	plan := validPlan(time.Now().UTC(), "duplicate-run")
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	duplicate := validPlan(time.Now().UTC(), "duplicate-run")
	duplicate.Run.WorkflowName = "replacement"
	require.Error(t, store.CreateRun(t.Context(), &duplicate))

	var workflowName string

	err := database.QueryRowContext(
		t.Context(),
		"SELECT workflow_name FROM cord_runs WHERE id = ?",
		plan.Run.ID,
	).Scan(&workflowName)
	require.NoError(t, err)
	assert.Equal(t, plan.Run.WorkflowName, workflowName)
	assert.Equal(t, 1, rowCount(t, database, runsTable))
}

func validPlan(now time.Time, runID storage.RunID) storage.RunPlan {
	const terminalNode = "publish"

	return storage.RunPlan{
		Run: storage.Run{
			CreatedAt:      now,
			UpdatedAt:      now,
			CompletedAt:    nil,
			ID:             runID,
			WorkflowName:   "build",
			DefinitionHash: "definition-hash",
			TerminalNodeID: terminalNode,
			Status:         storage.RunRunning,
			Input:          storage.EncodedPayload(`{"repository":"cord"}`),
			Output:         nil,
			Error:          nil,
		},
		Nodes: []storage.Node{
			newNode(
				runID,
				"compile",
				"example.com/workflow.Compile",
				"compile-signature",
				storage.NodeReady,
				now,
				0,
			),
			newNode(
				runID,
				terminalNode,
				"example.com/workflow.Publish",
				"publish-signature",
				storage.NodePending,
				now,
				1,
			),
		},
		Edges: []storage.Edge{{RunID: runID, Parent: "compile", Child: terminalNode, ParentOrder: 0}},
	}
}

func newNode(
	runID storage.RunID,
	nodeID storage.NodeID,
	functionKey string,
	signatureHash string,
	status storage.NodeStatus,
	availableAt time.Time,
	remainingDeps int,
) storage.Node {
	return storage.Node{
		AvailableAt:   availableAt,
		CompletedAt:   nil,
		StartedAt:     nil,
		SignatureHash: signatureHash,
		RunID:         runID,
		ID:            nodeID,
		FunctionKey:   functionKey,
		Status:        status,
		Lease:         storage.Lease{},
		Error:         nil,
		Output:        nil,
		RemainingDeps: remainingDeps,
		Attempt:       0,
	}
}

func newStore(t *testing.T, foreignKeys bool) (*sql.DB, *storage.Store) {
	t.Helper()

	database := openDatabase(t, foreignKeys)
	require.NoError(t, storage.Migrate(t.Context(), database))
	store, err := storage.NewStore(database)
	require.NoError(t, err)

	return database, store
}

func openDatabase(t *testing.T, foreignKeys bool) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", filepath.Join(t.TempDir(), "storage.db"))
	if foreignKeys {
		dsn += "&_pragma=foreign_keys(1)"
	}

	database, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, database.Close())
	})

	return database
}

func assertRowCounts(t *testing.T, database *sql.DB, expected map[string]int) {
	t.Helper()

	for table, expectedCount := range expected {
		assert.Equal(t, expectedCount, rowCount(t, database, table), table)
	}
}

func rowCount(t *testing.T, database *sql.DB, table string) int {
	t.Helper()

	statements := map[string]string{
		edgesTable: "SELECT COUNT(*) FROM cord_edges",
		nodesTable: "SELECT COUNT(*) FROM cord_nodes",
		runsTable:  "SELECT COUNT(*) FROM cord_runs",
	}

	require.Contains(t, statements, table)

	var count int

	err := database.QueryRowContext(t.Context(), statements[table]).Scan(&count)
	require.NoError(t, err)

	return count
}
