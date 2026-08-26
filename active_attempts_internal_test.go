package cord

import (
	"context"
	"sync"
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func TestCord_ActiveAttemptRegistryUnregistersWithoutLeaks(t *testing.T) {
	t.Parallel()

	runtime := &Cord{activeAttempts: make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)}
	claim := testClaim("registry-run", "registry-node")
	ctx, cancel := context.WithCancel(t.Context())

	runtime.activeMu.Lock()
	unregister := runtime.registerActiveAttemptLocked(claim, cancel)
	runtime.activeMu.Unlock()
	require.Len(t, runtime.activeAttempts[claim.RunID], 1)

	unregister()
	require.NotContains(t, runtime.activeAttempts, claim.RunID)

	unregister()
	require.NotContains(t, runtime.activeAttempts, claim.RunID)
	cancel()
	<-ctx.Done()
}

func TestCord_NotifyCompletionCancelsAllActiveAttempts(t *testing.T) {
	t.Parallel()

	runtime := &Cord{
		activeAttempts:    make(map[storage.RunID]map[activeAttemptKey]*activeAttempt),
		completionWaiters: make(map[storage.RunID]*completionPoll),
	}
	claim := testClaim("canceled-run", "active-node")
	contexts := make([]context.Context, 0, 2)

	runtime.activeMu.Lock()
	for range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		contexts = append(contexts, ctx)

		runtime.registerActiveAttemptLocked(claim, cancel)
	}
	runtime.activeMu.Unlock()

	runtime.notifyCompletion(claim.RunID)

	for _, ctx := range contexts {
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	}

	require.NotContains(t, runtime.activeAttempts, claim.RunID)
}

func TestCord_ActiveAttemptCancellationRacesUnregistration(t *testing.T) {
	t.Parallel()

	for range 100 {
		runtime := &Cord{activeAttempts: make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)}
		claim := testClaim("race-registry-run", "race-registry-node")
		ctx, cancel := context.WithCancel(t.Context())

		runtime.activeMu.Lock()
		unregister := runtime.registerActiveAttemptLocked(claim, cancel)
		runtime.activeMu.Unlock()

		var wait sync.WaitGroup

		wait.Add(2)

		go func() {
			defer wait.Done()

			runtime.cancelActiveAttempts(claim.RunID)
		}()
		go func() {
			defer wait.Done()

			unregister()
		}()

		wait.Wait()
		cancel()
		<-ctx.Done()
		require.NotContains(t, runtime.activeAttempts, claim.RunID)
	}
}

func TestCord_ActiveAttemptKeysSeparateClaims(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		change func(*storage.Claim)
		name   string
	}{
		{name: "generation", change: func(claim *storage.Claim) { claim.Lease.Generation++ }},
		{name: "node", change: func(claim *storage.Claim) { claim.NodeID = "other-node" }},
		{name: "run", change: func(claim *storage.Claim) { claim.RunID = "other-run" }},
		{name: "lease owner", change: func(claim *storage.Claim) { claim.Lease.Owner = "other-owner" }},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runtime := &Cord{activeAttempts: make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)}
			firstClaim := testClaim("key-run", "key-node")
			secondClaim := testClaim("key-run", "key-node")
			testCase.change(secondClaim)

			firstCtx, firstCancel := context.WithCancel(t.Context())
			secondCtx, secondCancel := context.WithCancel(t.Context())
			t.Cleanup(firstCancel)
			t.Cleanup(secondCancel)

			runtime.activeMu.Lock()
			unregisterFirst := runtime.registerActiveAttemptLocked(firstClaim, firstCancel)
			runtime.registerActiveAttemptLocked(secondClaim, secondCancel)
			runtime.activeMu.Unlock()

			unregisterFirst()
			unregisterFirst()
			runtime.cancelActiveAttempts(secondClaim.RunID)

			require.NoError(t, firstCtx.Err())
			require.ErrorIs(t, secondCtx.Err(), context.Canceled)
		})
	}
}

func TestCord_ActiveAttemptReplacementCannotBeRemovedByOldUnregister(t *testing.T) {
	t.Parallel()

	runtime := &Cord{activeAttempts: make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)}
	claim := testClaim("replacement-run", "replacement-node")
	oldCtx, oldCancel := context.WithCancel(t.Context())
	newCtx, newCancel := context.WithCancel(t.Context())

	runtime.activeMu.Lock()
	unregisterOld := runtime.registerActiveAttemptLocked(claim, oldCancel)
	runtime.registerActiveAttemptLocked(claim, newCancel)
	runtime.activeMu.Unlock()

	require.ErrorIs(t, oldCtx.Err(), context.Canceled)
	unregisterOld()
	runtime.cancelActiveAttempts(claim.RunID)
	require.ErrorIs(t, newCtx.Err(), context.Canceled)
	newCancel()
}
