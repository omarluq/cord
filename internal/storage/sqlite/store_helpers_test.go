package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
	"path/filepath"
	"testing"
	"time"
)

const (
	edgesTable = "cord_edges"
	nodesTable = "cord_nodes"
	runsTable  = "cord_runs"

	compileNode               storage.NodeID = "compile"
	terminalNode              storage.NodeID = "publish"
	testSubmissionFingerprint                = "submission-v1:abc"
)

func validPlan(now time.Time, runID storage.RunID) storage.RunPlan {
	return storage.RunPlan{
		Run: storage.Run{
			CreatedAt:             now,
			UpdatedAt:             now,
			CompletedAt:           nil,
			StartedAt:             nil,
			TerminalReason:        nil,
			TerminalRunnerID:      nil,
			ID:                    runID,
			WorkflowName:          "build",
			DefinitionHash:        "definition-hash",
			TerminalNodeID:        terminalNode,
			Status:                storage.RunRunning,
			Input:                 storage.EncodedPayload(`{"repository":"cord"}`),
			Output:                nil,
			Error:                 nil,
			MaxAttempts:           3,
			RetryBaseDelay:        500 * time.Millisecond,
			RetryMaxDelay:         30 * time.Second,
			RetryPolicyVersion:    1,
			IdempotencyKey:        nil,
			SubmissionFingerprint: nil,
		},
		Nodes: []storage.Node{
			newNode(
				runID,
				compileNode,
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
		Edges: []storage.Edge{{RunID: runID, Parent: compileNode, Child: terminalNode, ParentOrder: 0}},
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
		AvailableAt:    availableAt,
		CompletedAt:    nil,
		StartedAt:      nil,
		StateChangedAt: nil,
		LastStartedAt:  nil,
		LastRunnerID:   nil,
		TerminalReason: nil,
		SignatureHash:  signatureHash,
		RunID:          runID,
		ID:             nodeID,
		FunctionKey:    functionKey,
		Status:         status,
		Lease:          storage.Lease{},
		Error:          nil,
		Output:         nil,
		RemainingDeps:  remainingDeps,
		Attempt:        0,
	}
}

func newStore(t *testing.T, foreignKeys bool) (*sql.DB, *sqlite.Store) {
	t.Helper()

	database := openDatabase(t, foreignKeys)
	require.NoError(t, sqlite.Migrate(t.Context(), database))
	store, err := sqlite.New(database)
	require.NoError(t, err)

	return database, store
}

func openDatabase(t *testing.T, foreignKeys bool) *sql.DB {
	t.Helper()

	return openDatabaseAtPath(t, filepath.Join(t.TempDir(), "storage.db"), foreignKeys)
}

func openDatabaseAtPath(t *testing.T, path string, foreignKeys bool) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", path)
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

func requireCreateRun(
	ctx context.Context,
	tb testing.TB,
	store *sqlite.Store,
	plan *storage.RunPlan,
) {
	tb.Helper()
	require.NoError(tb, store.CreateRun(ctx, plan))
}

func requireCreateOrAttachRun(
	ctx context.Context,
	tb testing.TB,
	store *sqlite.Store,
	plan *storage.RunPlan,
) (storage.RunID, bool) {
	tb.Helper()

	runID, created, err := store.CreateOrAttachRun(ctx, plan)
	require.NoError(tb, err)

	return runID, created
}

func requireCreateRunError(
	ctx context.Context,
	tb testing.TB,
	store *sqlite.Store,
	plan *storage.RunPlan,
) {
	tb.Helper()
	require.Error(tb, store.CreateRun(ctx, plan))
}

func requireCreateRunErrorContains(
	ctx context.Context,
	tb testing.TB,
	store *sqlite.Store,
	plan *storage.RunPlan,
	message string,
) {
	tb.Helper()
	require.ErrorContains(tb, store.CreateRun(ctx, plan), message)
}
