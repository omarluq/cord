package sqlite_test

import (
	"context"
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
	"runtime"
	"sync"
	"sync/atomic"
)

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
