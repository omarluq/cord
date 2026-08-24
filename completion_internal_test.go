package cord

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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

func newResultStore(result *storage.RunResult) resultStore {
	return resultStore{value: *result, mu: sync.RWMutex{}}
}

func testRunResult(
	status storage.RunStatus,
	output storage.EncodedPayload,
) *storage.RunResult {
	return &storage.RunResult{
		WorkflowName:          "",
		DefinitionHash:        "",
		TerminalSignatureHash: "",
		Status:                status,
		Output:                output,
		Error:                 nil,
		MaxAttempts:           defaultMaxAttempts,
		RetryBaseDelay:        defaultBaseDelay,
		RetryMaxDelay:         defaultMaxDelay,
		RetryPolicyVersion:    retryPolicyVersion,
	}
}

func (store *resultStore) get() storage.RunResult {
	store.mu.RLock()
	defer store.mu.RUnlock()

	return store.value
}

func (store *resultStore) set(result *storage.RunResult) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.value = *result
}

type resultWaitBackend struct {
	storage.Backend
	reads      chan struct{}
	allowRead  <-chan struct{}
	result     resultStore
	totalReads atomic.Int64
}

type notifiedResult struct {
	err   error
	value int
}

func (backend *resultWaitBackend) GetRunResult(ctx context.Context, _ storage.RunID) (storage.RunResult, error) {
	backend.totalReads.Add(1)

	select {
	case backend.reads <- struct{}{}:
	default:
	}

	result := backend.result.get()
	if backend.allowRead != nil {
		select {
		case <-backend.allowRead:
		case <-ctx.Done():
			return storage.RunResult{}, fmt.Errorf("test result read: %w", ctx.Err())
		}
	}

	return result, nil
}

func newResultWaitRuntime(
	t *testing.T,
	result *storage.RunResult,
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
		completionWaiters: make(map[storage.RunID]*completionPoll),
	}
	codec, err := serialization.NewJSONCodec[int]()
	require.NoError(t, err)

	return runtime, backend, codec
}

func TestCompletionNotificationWakesAllWaitersAndCleansUp(t *testing.T) {
	t.Parallel()

	runtime := &Cord{
		store: &resultWaitBackend{
			result: newResultStore(testRunResult(storage.RunRunning, nil)),
			reads:  make(chan struct{}, 1),
		},
		ctx:               t.Context(),
		completionWaiters: make(map[storage.RunID]*completionPoll),
	}

	const runID storage.RunID = "notified-run"

	first, unsubscribeFirst := runtime.subscribeCompletion(runID, false)
	second, unsubscribeSecond := runtime.subscribeCompletion(runID, false)

	for index, waiter := range []<-chan completionObservation{first, second} {
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
		testRunResult(storage.RunRunning, nil),
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
	backend.result.set(testRunResult(storage.RunCompleted, []byte("42")))

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
		testRunResult(storage.RunCompleted, []byte("42")),
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

// BenchmarkCompletionNotification measures completion fanout at representative waiter counts.
func BenchmarkCompletionNotification(b *testing.B) {
	for _, waiterCount := range []int{1, 100, 10_000} {
		b.Run(fmt.Sprintf("waiters=%d", waiterCount), func(b *testing.B) {
			poll := &completionPoll{waiters: make(map[uint64]completionWaiter)}
			waiters := make([]<-chan completionObservation, 0, waiterCount)
			runtime := &Cord{completionWaiters: make(map[storage.RunID]*completionPoll)}

			for index := range waiterCount {
				waiter := make(chan completionObservation, 1)
				poll.waiters[uint64(index)] = completionWaiter{observations: waiter}
				waiters = append(waiters, waiter)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				runtime.publishCompletion(poll, &completionObservation{}, true)

				for _, waiter := range waiters {
					<-waiter
				}
			}
		})
	}
}
