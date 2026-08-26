package cord

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestWorkflowWaitPollsDurableRemoteCompletion(t *testing.T) {
	t.Parallel()

	runtime, backend, codec := newResultWaitRuntime(
		t,
		testRunResult(storage.RunRunning, nil),
		4,
	)

	done := make(chan error, 1)

	go func() {
		value, waitErr := (Workflow[int, int]{runtime: runtime}).wait(
			t.Context(), "remote-run", codec, false,
		)
		if waitErr == nil && value != 42 {
			waitErr = fmt.Errorf("unexpected result %d", value)
		}

		done <- waitErr
	}()

	<-backend.reads
	backend.result.set(testRunResult(storage.RunCompleted, []byte("42")))

	select {
	case waitErr := <-done:
		require.NoError(t, waitErr)
	case <-time.After(5 * resultPollInterval):
		t.Fatal("durable polling fallback did not observe remote completion")
	}

	require.Zero(t, completionWaiterCount(runtime))
}

// TestSameRunWaitersShareCompletionPolling verifies same-run waiters share durable reads.
func TestSameRunWaitersShareCompletionPolling(t *testing.T) {
	t.Parallel()

	for _, waiterCount := range []int{1, 100, 10_000} {
		t.Run(fmt.Sprintf("waiters=%d", waiterCount), func(t *testing.T) {
			t.Parallel()
			testSharedCompletionPolling(t, waiterCount)
		})
	}
}

func testSharedCompletionPolling(t *testing.T, waiterCount int) {
	t.Helper()

	readGate := make(chan struct{})
	backend := &resultWaitBackend{
		result:    newResultStore(testRunResult(storage.RunRunning, nil)),
		reads:     make(chan struct{}, 1),
		allowRead: readGate,
	}
	runtime := &Cord{
		store: backend, ctx: t.Context(),
		completionWaiters: make(map[storage.RunID]*completionPoll),
	}
	codec, err := serialization.NewJSONCodec[int]()
	require.NoError(t, err)

	outcomes := make(chan notifiedResult, waiterCount)
	for range waiterCount {
		go func() {
			value, waitErr := (Workflow[int, int]{runtime: runtime}).wait(
				t.Context(), "shared-run", codec, false,
			)
			outcomes <- notifiedResult{err: waitErr, value: value}
		}()
	}

	require.Eventually(t, func() bool {
		return completionWaiterCount(runtime) == waiterCount
	}, 5*time.Second, time.Millisecond)
	releaseCompletionRead(t, readGate, "initial completion poll did not start a result read")
	require.Eventually(t, func() bool {
		return backend.totalReads.Load() == 1
	}, time.Second, time.Millisecond)

	backend.result.set(testRunResult(storage.RunCompleted, []byte("42")))

	for range 100 {
		runtime.notifyCompletion("shared-run")
	}

	releaseCompletionRead(t, readGate, "completion notification did not start a result read")

	for range waiterCount {
		requireCompletionOutcome(t, outcomes)
	}

	require.EqualValues(t, 2, backend.totalReads.Load())
	require.Zero(t, completionWaiterCount(runtime))
	require.Zero(t, completionPollCount(runtime))
}

func releaseCompletionRead(t *testing.T, readGate chan<- struct{}, failure string) {
	t.Helper()

	select {
	case readGate <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal(failure)
	}
}

func requireCompletionOutcome(t *testing.T, outcomes <-chan notifiedResult) {
	t.Helper()

	select {
	case outcome := <-outcomes:
		require.NoError(t, outcome.err)
		require.Equal(t, 42, outcome.value)
	case <-time.After(5 * time.Second):
		t.Fatal("completion waiter did not return")
	}
}

func TestCompletionWaiterCleanupOnCancelAndClose(t *testing.T) {
	t.Parallel()

	backend := &resultWaitBackend{
		result: newResultStore(testRunResult(storage.RunRunning, nil)),
		reads:  make(chan struct{}, 4),
	}
	runtime := newAdmissionTestRuntime(backend)
	codec, err := serialization.NewJSONCodec[int]()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	waitDone := make(chan error, 1)

	go func() {
		_, waitErr := (Workflow[int, int]{runtime: runtime}).wait(ctx, "cancel-run", codec, true)
		waitDone <- waitErr
	}()

	<-backend.reads
	cancel()
	require.ErrorIs(t, <-waitDone, context.Canceled)
	require.Zero(t, completionWaiterCount(runtime))

	waiter, unsubscribe := runtime.subscribeCompletion("close-run", true)
	require.NotNil(t, waiter)
	require.Equal(t, 1, completionWaiterCount(runtime))
	require.NoError(t, runtime.Close())
	require.Zero(t, completionWaiterCount(runtime))
	unsubscribe()
}

func completionWaiterCount(runtime *Cord) int {
	runtime.waiterMu.Lock()
	defer runtime.waiterMu.Unlock()

	count := 0
	for _, poll := range runtime.completionWaiters {
		count += len(poll.waiters)
	}

	return count
}

func completionPollCount(runtime *Cord) int {
	runtime.waiterMu.Lock()
	defer runtime.waiterMu.Unlock()

	return len(runtime.completionWaiters)
}
