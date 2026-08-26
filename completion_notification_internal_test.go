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
