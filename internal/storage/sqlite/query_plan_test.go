package sqlite_test

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// TestSQLiteQueryPlans_TimestampPredicates verifies timestamp predicates use the expected indexes.
func TestSQLiteQueryPlans_TimestampPredicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		query             string
		wantIndexDetail   string
		wantTemporarySort bool
	}{
		{
			name: "production claim predicate",
			query: `SELECT run_id, node_id FROM cord_nodes
				WHERE status = 'ready' AND julianday(available_at) <= julianday('now')
				ORDER BY julianday(available_at), run_id, node_id LIMIT 1`,
			wantIndexDetail:   "cord_nodes_status_available_at_idx (status=?)",
			wantTemporarySort: true,
		},
		{
			name: "index-compatible comparison candidate",
			query: `SELECT run_id, node_id FROM cord_nodes
				WHERE status = 'ready' AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
				ORDER BY available_at, run_id, node_id LIMIT 1`,
			wantIndexDetail:   "cord_nodes_status_available_at_idx (status=? AND available_at<?)",
			wantTemporarySort: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			database, _ := newStore(t, true)
			details := explainQueryPlan(t, database, testCase.query)
			joined := strings.Join(details, "\n")
			assert.Contains(t, joined, testCase.wantIndexDetail)
			assert.Equal(t, testCase.wantTemporarySort, strings.Contains(joined, "USE TEMP B-TREE"), joined)
		})
	}
}

// TestSQLiteQueryPlans_NodeInspectionPages verifies node inspection pages use keyset indexes.
// TestSQLiteQueryPlan_RunInspectionCounts verifies run inspection counts use run and status indexes.
// TestSQLiteQueryPlan_OrderedParentInputs verifies ordered parent inputs avoid a temporary sort.
func TestSQLiteQueryPlan_OrderedParentInputs(t *testing.T) {
	t.Parallel()

	database, _ := newStore(t, true)
	query := `SELECT p.output_payload FROM cord_edges AS e
		JOIN cord_nodes AS p ON p.run_id = e.run_id AND p.node_id = e.parent_node_id
		WHERE e.run_id = 'run' AND e.child_node_id = 'child' ORDER BY e.parent_order`

	_, err := database.ExecContext(t.Context(), "DROP INDEX cord_edges_run_child_parent_order_idx")
	require.NoError(t, err)

	before := strings.Join(explainQueryPlan(t, database, query), "\n")
	assert.Contains(t, before, "sqlite_autoindex_cord_edges_1 (run_id=?)")
	assert.Contains(t, before, "USE TEMP B-TREE FOR ORDER BY")

	_, err = database.ExecContext(t.Context(), `CREATE INDEX cord_edges_run_child_parent_order_idx
		ON cord_edges(run_id, child_node_id, parent_order)`)
	require.NoError(t, err)

	after := strings.Join(explainQueryPlan(t, database, query), "\n")
	assert.Contains(t, after,
		"cord_edges_run_child_parent_order_idx (run_id=? AND child_node_id=?)")
	assert.NotContains(t, after, "USE TEMP B-TREE FOR ORDER BY")
}

func explainQueryPlan(t *testing.T, database *sql.DB, query string) []string {
	t.Helper()

	rows, err := database.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query)

	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var details []string

	for rows.Next() {
		var (
			id, parent, unused int
			detail             string
		)
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}

	require.NoError(t, rows.Err())

	return details
}
