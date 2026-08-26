package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
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
		path := filepath.Join(t.TempDir(), testCase.name+"-claims.db")
		databases, stores := openClaimStores(t, path, claimants)
		createReadyRuns(t, stores[0], runs, "run")

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		claims, progress, errs := claimQuotaConcurrently(
			ctx,
			stores,
			runs/claimants,
			testCase.registered,
		)

		for _, err := range errs {
			require.NoError(t, err, testCase.name)
		}

		require.NoError(t, ctx.Err(), testCase.name)
		cancel()
		assert.Len(t, uniqueClaimedNodes(t, claims), runs, testCase.name)

		for index, count := range progress {
			assert.Equal(t, int64(runs/claimants), count, "%s worker-%d progress", testCase.name, index)
		}

		assert.Equal(t, runs, runningNodeCount(t, databases[0], compileNode), testCase.name)
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
