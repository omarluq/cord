package sqlite_test

import (
	"database/sql"
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func createReadyRuns(tb testing.TB, store *sqlite.Store, count int, prefix string) {
	tb.Helper()

	now := time.Now().UTC()
	for index := range count {
		plan := validPlan(now, storage.RunID(fmt.Sprintf("%s-%03d", prefix, index)))
		requireCreateRun(tb.Context(), tb, store, &plan)
	}
}

func uniqueClaimedNodes(t *testing.T, claims []*storage.Claim) map[string]struct{} {
	t.Helper()

	claimedNodes := make(map[string]struct{}, len(claims))
	for _, claim := range claims {
		key := string(claim.RunID) + "/" + string(claim.NodeID)
		assert.NotContains(t, claimedNodes, key)
		claimedNodes[key] = struct{}{}
	}

	return claimedNodes
}

func runningNodeCount(t *testing.T, database *sql.DB, nodeID storage.NodeID) int {
	t.Helper()

	var count int
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_nodes WHERE node_id = ? AND status = ?",
		nodeID,
		storage.NodeRunning,
	).Scan(&count))

	return count
}

func collect[T any](values <-chan T) []T {
	collected := make([]T, 0, len(values))
	for value := range values {
		collected = append(collected, value)
	}

	return collected
}

func openClaimStores(tb testing.TB, path string, count int) ([]*sql.DB, []*sqlite.Store) {
	tb.Helper()

	databases := make([]*sql.DB, 0, count)
	for range count {
		database, err := sql.Open(
			"sqlite",
			"file:"+path+"?_pragma=busy_timeout(100)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)",
		)
		require.NoError(tb, err)
		tb.Cleanup(func() { require.NoError(tb, database.Close()) })

		databases = append(databases, database)
	}

	require.NoError(tb, sqlite.Migrate(tb.Context(), databases[0]))

	stores := make([]*sqlite.Store, 0, count)

	for _, database := range databases {
		store, err := sqlite.New(database)
		require.NoError(tb, err)

		stores = append(stores, store)
	}

	return databases, stores
}
