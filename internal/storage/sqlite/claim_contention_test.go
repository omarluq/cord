package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const claimLeaseTTL = time.Minute

func TestStore_ConcurrentClaimantsMakeProgressWithoutDuplicateClaims(t *testing.T) {
	t.Parallel()

	const (
		claimants = 8
		runs      = 64
	)

	tests := []struct {
		name       string
		registered []storage.FunctionRegistration
	}{
		{name: "all functions", registered: nil},
		{
			name: "registered functions",
			registered: []storage.FunctionRegistration{
				{Key: "example.com/workflow.Compile", Signature: "compile-signature"},
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "claims.db")
			databases, stores := openClaimStores(t, path, claimants)
			createReadyRuns(t, stores[0], runs, "run")

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			claims, progress, errs := claimQuotaConcurrently(
				ctx,
				stores,
				runs/claimants,
				testCase.registered,
			)
			for _, err := range errs {
				require.NoError(t, err)
			}

			require.NoError(t, ctx.Err())
			assert.Len(t, uniqueClaimedNodes(t, claims), runs)

			for index, count := range progress {
				assert.Equal(t, int64(runs/claimants), count, "worker-%d progress", index)
			}

			assert.Equal(t, runs, runningNodeCount(t, databases[0], compileNode))
		})
	}
}

func TestStore_ClaimRetriesSQLiteContentionUntilLockIsReleased(t *testing.T) {
	t.Parallel()

	store, transaction, runID := setupContendedClaim(t, "retry-contended-claim")

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	result := claimRegisteredReadyNodeAsync(ctx, store)

	select {
	case earlyResult := <-result:
		require.FailNow(t, "claim returned while the write lock was held", earlyResult.err)
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, transaction.Rollback())

	resultValue := <-result
	require.NoError(t, resultValue.err)
	require.True(t, resultValue.claimed)
	require.NotNil(t, resultValue.claim)
	assert.Equal(t, runID, resultValue.claim.RunID)
}

func TestStore_ClaimStopsRetryingSQLiteContentionOnCancellation(t *testing.T) {
	t.Parallel()

	store, transaction, _ := setupContendedClaim(t, "contended-claim")
	defer func() { require.NoError(t, transaction.Rollback()) }()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	claim, claimed, err := store.ClaimReadyNode(ctx, "worker", claimLeaseTTL)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, claimed)
	assert.Nil(t, claim)
}

func setupContendedClaim(t *testing.T, runID storage.RunID) (*sqlite.Store, *sql.Tx, storage.RunID) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "contended-claim.db")
	first := openZeroTimeoutDatabase(t, path)
	second := openZeroTimeoutDatabase(t, path)
	require.NoError(t, sqlite.Migrate(t.Context(), first))

	store, err := sqlite.New(second)
	require.NoError(t, err)

	plan := validPlan(time.Now().UTC(), runID)
	requireCreateRun(t.Context(), t, store, &plan)

	transaction, err := first.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			t.Errorf("rollback contended transaction: %v", rollbackErr)
		}
	})

	_, err = transaction.ExecContext(t.Context(), "UPDATE cord_runs SET status = status")
	require.NoError(t, err)

	return store, transaction, plan.Run.ID
}

type claimResult struct {
	err     error
	claim   *storage.Claim
	claimed bool
}

func claimRegisteredReadyNodeAsync(ctx context.Context, store *sqlite.Store) <-chan claimResult {
	result := make(chan claimResult, 1)

	go func() {
		registered := []storage.FunctionRegistration{
			{Key: "example.com/workflow.Compile", Signature: "compile-signature"},
		}

		claim, claimed, err := store.ClaimReadyNodeForFunctions(
			ctx,
			"worker",
			claimLeaseTTL,
			registered,
		)
		result <- claimResult{claim: claim, claimed: claimed, err: err}
	}()

	return result
}

func BenchmarkStore_ConcurrentClaims(b *testing.B) {
	for _, claimants := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("claimants=%d", claimants), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "claims.db")
			_, stores := openClaimStores(b, path, claimants)
			createReadyRuns(b, stores[0], b.N, "benchmark")

			b.ReportAllocs()
			b.ResetTimer()

			claimed, errs := claimAllConcurrently(b.Context(), stores, b.N)

			b.StopTimer()

			for _, err := range errs {
				require.NoError(b, err)
			}

			require.Equal(b, int64(b.N), claimed)
		})
	}
}

func claimQuotaConcurrently(
	ctx context.Context,
	stores []*sqlite.Store,
	quota int,
	registered []storage.FunctionRegistration,
) ([]*storage.Claim, []int64, []error) {
	claims := make(chan *storage.Claim, len(stores)*quota)
	errs := make(chan error, len(stores))
	progress := make([]atomic.Int64, len(stores))

	var workers sync.WaitGroup

	for index, store := range stores {
		worker := quotaWorker{
			store:      store,
			registered: registered,
			progress:   &progress[index],
			claims:     claims,
			errs:       errs,
			index:      index,
			quota:      quota,
		}

		workers.Go(func() {
			worker.claim(ctx)
		})
	}

	workers.Wait()
	close(claims)
	close(errs)

	counts := make([]int64, len(progress))
	for index := range progress {
		counts[index] = progress[index].Load()
	}

	return collect(claims), counts, collect(errs)
}

type quotaWorker struct {
	store      *sqlite.Store
	progress   *atomic.Int64
	claims     chan<- *storage.Claim
	errs       chan<- error
	registered []storage.FunctionRegistration
	index      int
	quota      int
}

func (w quotaWorker) claim(ctx context.Context) {
	owner := fmt.Sprintf("worker-%d", w.index)
	for w.progress.Load() < int64(w.quota) {
		claim, won, err := claimReadyNode(ctx, w.store, owner, w.registered)
		if err != nil {
			w.errs <- err

			return
		}

		if !won {
			runtime.Gosched()

			continue
		}

		w.progress.Add(1)

		w.claims <- claim
	}
}

func claimReadyNode(
	ctx context.Context,
	store *sqlite.Store,
	owner string,
	registered []storage.FunctionRegistration,
) (*storage.Claim, bool, error) {
	if len(registered) > 0 {
		claim, claimed, err := store.ClaimReadyNodeForFunctions(ctx, owner, claimLeaseTTL, registered)
		if err != nil {
			return nil, false, fmt.Errorf("claim registered ready node: %w", err)
		}

		return claim, claimed, nil
	}

	claim, claimed, err := store.ClaimReadyNode(ctx, owner, claimLeaseTTL)
	if err != nil {
		return nil, false, fmt.Errorf("claim ready node: %w", err)
	}

	return claim, claimed, nil
}

func claimAllConcurrently(ctx context.Context, stores []*sqlite.Store, total int) (int64, []error) {
	var claimed atomic.Int64

	var workers sync.WaitGroup

	errs := make(chan error, len(stores))
	for index, store := range stores {
		workers.Go(func() {
			claimUntilTotal(ctx, store, index, int64(total), &claimed, errs)
		})
	}

	workers.Wait()
	close(errs)

	return claimed.Load(), collect(errs)
}

func claimUntilTotal(
	ctx context.Context,
	store *sqlite.Store,
	worker int,
	total int64,
	claimed *atomic.Int64,
	errs chan<- error,
) {
	for claimed.Load() < total {
		_, won, err := store.ClaimReadyNode(ctx, fmt.Sprintf("worker-%d", worker), claimLeaseTTL)
		if err != nil {
			errs <- err

			return
		}

		if won {
			claimed.Add(1)
		}
	}
}

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
