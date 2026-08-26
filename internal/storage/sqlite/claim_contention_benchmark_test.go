package sqlite_test

import (
	"context"
	"fmt"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

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
