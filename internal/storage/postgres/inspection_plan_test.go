package postgres_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	postgresstore "github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresInspectionUsesRunScopedCountPlan(t *testing.T) {
	t.Parallel()

	database := prepareInspectionPlanDatabase(t)

	plan := explainPostgres(t, database, postgresstore.InspectRunQueryForTest(), "plan")
	assert.Contains(t, plan, "cord_nodes_run_status_idx")
	assert.Contains(t, plan, "Index Cond: (run_id = r.id)")
	assert.NotContains(t, plan, "Seq Scan on cord_nodes")
	assert.NotContains(t, plan, "Rows Removed by Filter")
}

func TestPostgresNodePageUsesPrimaryKeyKeysetPlan(t *testing.T) {
	t.Parallel()

	database := prepareInspectionPlanDatabase(t)

	tests := []struct {
		state  any
		reason any
		name   string
	}{
		{name: "unfiltered", state: nil, reason: nil},
		{name: "state", state: storage.NodeReady, reason: nil},
		{name: "reason", state: nil, reason: storage.ReasonSucceeded},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			plan := explainPostgres(t, database, postgresstore.NodePageQueryForTest(),
				"plan", "node-005", testCase.state, testCase.reason, 6)
			assert.Contains(t, plan, "cord_nodes_pkey")
			assert.Contains(t, plan, "node_id > 'node-005'::text")
			assert.NotContains(t, strings.ToLower(plan), "sort")
		})
	}
}

func explainPostgres(t *testing.T, database *sql.DB, statement string, arguments ...any) string {
	t.Helper()

	transaction, err := database.BeginTx(t.Context(), nil)

	require.NoError(t, err)
	defer func() { require.NoError(t, transaction.Rollback()) }()

	_, err = transaction.ExecContext(t.Context(), "SET LOCAL enable_seqscan = off")
	require.NoError(t, err)

	rows, err := transaction.QueryContext(
		t.Context(),
		"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) "+statement,
		arguments...,
	)

	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var plan strings.Builder

	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}

	require.NoError(t, rows.Err())

	return plan.String()
}

func prepareInspectionPlanDatabase(t *testing.T) *sql.DB {
	t.Helper()

	database := openInspectionPostgres(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	insertInspectionRun(t, database, &inspectionRun{
		runID: "plan", status: storage.RunRunning, reason: nil, now: now, finishedAt: nil,
	})

	for index := range 20 {
		insertInspectionNode(t, database, "plan", fmt.Sprintf("node-%03d", index), storage.NodeReady, now)
	}

	analyzeInspectionTables(t, database)

	return database
}

func analyzeInspectionTables(t *testing.T, database *sql.DB) {
	t.Helper()

	_, err := database.ExecContext(t.Context(), "ANALYZE cord_runs, cord_nodes")
	require.NoError(t, err)
}
