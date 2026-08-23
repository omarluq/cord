package cord

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
)

type resultStore struct {
	value storage.RunResult
	mu    sync.RWMutex
}

func newResultStore(result storage.RunResult) resultStore {
	return resultStore{value: result, mu: sync.RWMutex{}}
}

func (store *resultStore) get() storage.RunResult {
	store.mu.RLock()
	defer store.mu.RUnlock()

	return store.value
}

func (store *resultStore) set(result storage.RunResult) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.value = result
}

type resultWaitBackend struct {
	storage.Backend
	reads  chan struct{}
	result resultStore
}

type notifiedResult struct {
	err   error
	value int
}

func (backend *resultWaitBackend) GetRunResult(context.Context, storage.RunID) (storage.RunResult, error) {
	select {
	case backend.reads <- struct{}{}:
	default:
	}

	return backend.result.get(), nil
}

func newResultWaitRuntime(
	t *testing.T,
	result storage.RunResult,
	readCapacity int,
) (*Cord, *resultWaitBackend, serialization.JSONCodec[int]) {
	t.Helper()

	backend := &resultWaitBackend{
		result: newResultStore(result),
		reads:  make(chan struct{}, readCapacity),
	}
	runtime := &Cord{
		store:             backend,
		ctx:               t.Context(),
		completionWaiters: make(map[storage.RunID]map[uint64]chan struct{}),
	}
	codec, err := serialization.NewJSONCodec[int]()
	require.NoError(t, err)

	return runtime, backend, codec
}

func TestCompletionNotificationWakesAllWaitersAndCleansUp(t *testing.T) {
	t.Parallel()

	runtime := &Cord{
		ctx:               t.Context(),
		completionWaiters: make(map[storage.RunID]map[uint64]chan struct{}),
	}

	const runID storage.RunID = "notified-run"

	first, unsubscribeFirst := runtime.subscribeCompletion(runID)
	second, unsubscribeSecond := runtime.subscribeCompletion(runID)

	runtime.notifyCompletion(runID)

	for index, waiter := range []<-chan struct{}{first, second} {
		select {
		case <-waiter:
		case <-time.After(time.Second):
			t.Fatalf("waiter %d was not notified", index)
		}
	}

	unsubscribeFirst()
	unsubscribeSecond()
	require.Zero(t, completionWaiterCount(runtime))
}

func TestWorkflowWaitUsesLocalNotification(t *testing.T) {
	t.Parallel()

	runtime, backend, codec := newResultWaitRuntime(
		t,
		storage.RunResult{Status: storage.RunRunning, Output: nil, Error: nil},
		4,
	)

	done := make(chan notifiedResult, 1)

	go func() {
		value, waitErr := (Workflow[int, int]{runtime: runtime}).wait(
			t.Context(), "local-run", codec, false,
		)
		done <- notifiedResult{err: waitErr, value: value}
	}()

	<-backend.reads
	backend.result.set(storage.RunResult{Status: storage.RunCompleted, Output: []byte("42"), Error: nil})

	started := time.Now()

	runtime.notifyCompletion("local-run")

	select {
	case outcome := <-done:
		require.NoError(t, outcome.err)
		require.Equal(t, 42, outcome.value)
		require.Less(t, time.Since(started), resultPollInterval)
	case <-time.After(time.Second):
		t.Fatal("local completion notification did not wake Run")
	}

	require.Zero(t, completionWaiterCount(runtime))
}

func TestWorkflowWaitNotificationBeforeSubscriptionFallsBackToDurableRead(t *testing.T) {
	t.Parallel()

	runtime, _, codec := newResultWaitRuntime(
		t,
		storage.RunResult{Status: storage.RunCompleted, Output: []byte("42"), Error: nil},
		1,
	)

	runtime.notifyCompletion("missed-run")
	value, err := (Workflow[int, int]{runtime: runtime}).wait(t.Context(), "missed-run", codec, false)
	require.NoError(t, err)
	require.Equal(t, 42, value)
	require.Zero(t, completionWaiterCount(runtime))
}

func TestWorkflowWaitPollsDurableRemoteCompletion(t *testing.T) {
	t.Parallel()

	runtime, backend, codec := newResultWaitRuntime(
		t,
		storage.RunResult{Status: storage.RunRunning, Output: nil, Error: nil},
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
	backend.result.set(storage.RunResult{Status: storage.RunCompleted, Output: []byte("42"), Error: nil})

	select {
	case waitErr := <-done:
		require.NoError(t, waitErr)
	case <-time.After(5 * resultPollInterval):
		t.Fatal("durable polling fallback did not observe remote completion")
	}

	require.Zero(t, completionWaiterCount(runtime))
}

func TestCompletionWaiterCleanupOnCancelAndClose(t *testing.T) {
	t.Parallel()

	backend := &resultWaitBackend{
		result: newResultStore(storage.RunResult{Status: storage.RunRunning, Output: nil, Error: nil}),
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

	waiter, unsubscribe := runtime.subscribeCompletion("close-run")
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
	for _, waiters := range runtime.completionWaiters {
		count += len(waiters)
	}

	return count
}

func BenchmarkCompletionNotification(b *testing.B) {
	for _, waiterCount := range []int{1, 100} {
		b.Run(fmt.Sprintf("waiters=%d", waiterCount), func(b *testing.B) {
			runtime := &Cord{
				ctx:               b.Context(),
				completionWaiters: make(map[storage.RunID]map[uint64]chan struct{}),
			}

			waiters := make([]<-chan struct{}, 0, waiterCount)

			for range waiterCount {
				waiter, _ := runtime.subscribeCompletion("benchmark-run")
				waiters = append(waiters, waiter)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				runtime.notifyCompletion("benchmark-run")

				for _, waiter := range waiters {
					<-waiter
				}
			}
		})
	}
}
