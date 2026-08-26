package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

const dropEdges = `DROP TABLE cord_edges`

type incompatibleSchemaCase struct {
	name       string
	wantError  string
	statements []string
}

func TestVerifyRejectsStructurallyIncompatibleCurrentSchema(t *testing.T) {
	t.Parallel()

	for _, testCase := range incompatibleSchemaCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			runIncompatibleSchemaCase(t, testCase)
		})
	}
}

func incompatibleSchemaCases() []incompatibleSchemaCase {
	cases := baseIncompatibleSchemaCases()

	return append(cases, identityIncompatibleSchemaCases()...)
}

func baseIncompatibleSchemaCases() []incompatibleSchemaCase {
	return []incompatibleSchemaCase{
		{
			name:       "missing column",
			statements: []string{`ALTER TABLE cord_runs RENAME COLUMN max_attempts TO missing_max_attempts`},
			wantError:  `column "cord_runs"."max_attempts"`,
		},
		{
			name: "wrong column affinity",
			statements: []string{
				dropEdges,
				`CREATE TABLE cord_edges (
					run_id TEXT NOT NULL,
					parent_node_id TEXT NOT NULL,
					child_node_id TEXT NOT NULL,
					parent_order TEXT NOT NULL DEFAULT 0,
					PRIMARY KEY (run_id, parent_node_id, child_node_id),
					FOREIGN KEY (run_id, parent_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE,
					FOREIGN KEY (run_id, child_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE
				)`,
			},
			wantError: `column "cord_edges"."parent_order"`,
		},
		{
			name: "missing primary key",
			statements: []string{
				dropEdges,
				`CREATE TABLE cord_edges (
					run_id TEXT NOT NULL,
					parent_node_id TEXT NOT NULL,
					child_node_id TEXT NOT NULL,
					parent_order INTEGER NOT NULL DEFAULT 0,
					FOREIGN KEY (run_id, parent_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE,
					FOREIGN KEY (run_id, child_node_id) REFERENCES cord_nodes(run_id, node_id) ON DELETE CASCADE
				)`,
			},
			wantError: `column "cord_edges"."run_id"`,
		},
		{
			name:       "missing node index",
			statements: []string{`DROP INDEX cord_nodes_lease_expires_at_idx`},
			wantError:  `index "cord_nodes_lease_expires_at_idx" is missing`,
		},
		{
			name:       "missing ordered-child edge index",
			statements: []string{`DROP INDEX cord_edges_run_child_parent_order_idx`},
			wantError:  `index "cord_edges_run_child_parent_order_idx" is missing`,
		},
		{
			name: "wrong index columns",
			statements: []string{
				`DROP INDEX cord_nodes_run_status_idx`,
				`CREATE INDEX cord_nodes_run_status_idx ON cord_nodes(status, run_id)`,
			},
			wantError: `index "cord_nodes_run_status_idx" has columns [status run_id]`,
		},
		{
			name: "partial required index",
			statements: []string{
				`DROP INDEX cord_nodes_run_status_idx`,
				`CREATE INDEX cord_nodes_run_status_idx ON cord_nodes(run_id, status) WHERE status = 'running'`,
			},
			wantError: `index "cord_nodes_run_status_idx" has columns [run_id status], unique=false, and partial=true`,
		},
		{
			name: "missing foreign keys",
			statements: []string{
				dropEdges,
				`CREATE TABLE cord_edges (
					run_id TEXT NOT NULL,
					parent_node_id TEXT NOT NULL,
					child_node_id TEXT NOT NULL,
					parent_order INTEGER NOT NULL DEFAULT 0,
					PRIMARY KEY (run_id, parent_node_id, child_node_id)
				)`,
				`CREATE INDEX cord_edges_run_child_parent_order_idx
					ON cord_edges(run_id, child_node_id, parent_order)`,
			},
			wantError: `table "cord_edges" is missing foreign key`,
		},
	}
}

func identityIncompatibleSchemaCases() []incompatibleSchemaCase {
	return []incompatibleSchemaCase{
		{
			name:       "missing submission identity column",
			statements: []string{`ALTER TABLE cord_runs RENAME COLUMN submission_fingerprint TO missing_fingerprint`},
			wantError:  `column "cord_runs"."submission_fingerprint"`,
		},
		{
			name:       "missing idempotency index",
			statements: []string{`DROP INDEX cord_runs_workflow_name_idempotency_key_idx`},
			wantError:  `index "cord_runs_workflow_name_idempotency_key_idx" is missing`,
		},
		{
			name: "non-unique idempotency index",
			statements: []string{
				`DROP INDEX cord_runs_workflow_name_idempotency_key_idx`,
				`CREATE INDEX cord_runs_workflow_name_idempotency_key_idx
					ON cord_runs(workflow_name, idempotency_key)`,
			},
			wantError: `index "cord_runs_workflow_name_idempotency_key_idx" has columns ` +
				`[workflow_name idempotency_key], unique=false`,
		},
	}
}

func runIncompatibleSchemaCase(t *testing.T, testCase incompatibleSchemaCase) {
	t.Helper()
	database := openDatabase(t, false)
	require.NoError(t, sqlite.Migrate(t.Context(), database))

	for _, statement := range testCase.statements {
		_, err := database.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}

	err := sqlite.Verify(t.Context(), database)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema is incompatible")
	assert.Contains(t, err.Error(), testCase.wantError)
	require.NotErrorIs(t, err, storage.ErrSchemaOutdated)
	require.NotErrorIs(t, err, storage.ErrSchemaNewer)

	err = sqlite.Migrate(t.Context(), database)
	require.Error(t, err)
	assert.Contains(t, err.Error(), testCase.wantError)
}
