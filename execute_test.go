package cord

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecute_BoundsConcurrency(t *testing.T) {
	t.Parallel()

	const (
		nodeCount   = 8
		concurrency = 2
	)

	var (
		active atomic.Int32
		peak   atomic.Int32
	)

	started := make(chan struct{}, nodeCount)
	release := make(chan struct{})
	plan := make([]node, 0, nodeCount+1)
	plan = append(plan, node{
		id:      1,
		parents: []nodeID{},
		invoke: func(_ context.Context, _ []any) (any, error) {
			return 1, nil
		},
	})

	parents := make([]nodeID, 0, nodeCount)
	for index := range nodeCount {
		id := nodeID(index + 2)
		parents = append(parents, id)
		plan = append(plan, node{
			id:      id,
			parents: []nodeID{1},
			invoke: func(ctx context.Context, _ []any) (any, error) {
				current := active.Add(1)
				defer active.Add(-1)

				updatePeak(&peak, current)

				started <- struct{}{}

				select {
				case <-release:
					return 1, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		})
	}

	tail := nodeID(nodeCount + 2)
	plan = append(plan, node{
		id:      tail,
		parents: parents,
		invoke: func(_ context.Context, inputs []any) (any, error) {
			return len(inputs), nil
		},
	})

	done := make(chan struct{})

	var (
		output any
		runErr error
	)
	go func() {
		output, runErr = execute(t.Context(), plan, tail, 0, make(chan struct{}, concurrency))

		close(done)
	}()

	for range concurrency {
		receiveSignal(t, started, "waiting for workflow node to start")
	}

	assert.EqualValues(t, concurrency, active.Load())
	assert.EqualValues(t, concurrency, peak.Load())
	close(release)
	receiveSignal(t, done, "waiting for workflow execution to finish")

	require.NoError(t, runErr)
	assert.Equal(t, nodeCount, output)
}

func TestExecute_BoundsConcurrencyAcrossRuns(t *testing.T) {
	t.Parallel()

	const (
		runCount    = 4
		concurrency = 2
	)

	var (
		active atomic.Int32
		peak   atomic.Int32
	)

	started := make(chan struct{}, runCount)
	release := make(chan struct{})
	slots := make(chan struct{}, concurrency)
	plan := []node{
		{
			id:      1,
			parents: []nodeID{},
			invoke: func(ctx context.Context, _ []any) (any, error) {
				current := active.Add(1)
				defer active.Add(-1)

				updatePeak(&peak, current)

				started <- struct{}{}

				select {
				case <-release:
					return 1, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		},
	}

	var waitGroup sync.WaitGroup
	for range runCount {
		waitGroup.Go(func() {
			_, err := execute(t.Context(), plan, 1, 0, slots)
			assert.NoError(t, err)
		})
	}

	for range concurrency {
		receiveSignal(t, started, "waiting for concurrent workflow run to start")
	}

	assert.EqualValues(t, concurrency, active.Load())

	select {
	case <-started:
		require.FailNow(t, "runtime concurrency limit was exceeded")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	waitGroup.Wait()
	assert.EqualValues(t, concurrency, peak.Load())
}

func TestInputAs_HandlesNilByTargetType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func() bool
		name  string
		want  bool
	}{
		{name: "interface", check: func() bool {
			_, ok := inputAs[any](nil)

			return ok
		}, want: true},
		{name: "pointer", check: func() bool {
			_, ok := inputAs[*int](nil)

			return ok
		}, want: true},
		{name: "map", check: func() bool {
			_, ok := inputAs[map[string]int](nil)

			return ok
		}, want: true},
		{name: "slice", check: func() bool {
			_, ok := inputAs[[]int](nil)

			return ok
		}, want: true},
		{name: "function", check: func() bool {
			_, ok := inputAs[func()](nil)

			return ok
		}, want: true},
		{name: "channel", check: func() bool {
			_, ok := inputAs[chan int](nil)

			return ok
		}, want: true},
		{name: "integer", check: func() bool {
			_, ok := inputAs[int](nil)

			return ok
		}, want: false},
		{name: "boolean", check: func() bool {
			_, ok := inputAs[bool](nil)

			return ok
		}, want: false},
		{name: "struct", check: func() bool {
			_, ok := inputAs[struct{}](nil)

			return ok
		}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, test.check())
		})
	}

	var pointer *int

	value, ok := inputAs[*int](pointer)
	assert.True(t, ok)
	assert.Nil(t, value)
}

func TestExecute_PanicPreservesErrorIdentity(t *testing.T) {
	t.Parallel()

	expected := errors.New("panic cause")
	plan := []node{{
		id:      1,
		parents: []nodeID{},
		invoke: func(_ context.Context, _ []any) (any, error) {
			panic(expected)
		},
	}}

	slots := make(chan struct{}, 1)
	_, err := execute(t.Context(), plan, 1, nil, slots)

	require.ErrorIs(t, err, expected)
	require.ErrorContains(t, err, "workflow step panicked")
	assert.Empty(t, slots)

	successPlan := []node{{
		id:      1,
		parents: []nodeID{},
		invoke: func(_ context.Context, _ []any) (any, error) {
			return "ok", nil
		},
	}}
	output, err := execute(t.Context(), successPlan, 1, nil, slots)
	require.NoError(t, err)
	assert.Equal(t, "ok", output)
}

func TestExecute_WaitsForSiblingAfterPanic(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	stopped := make(chan struct{})
	plan := []node{
		{
			id:      1,
			parents: []nodeID{},
			invoke: func(_ context.Context, _ []any) (any, error) {
				return 1, nil
			},
		},
		{
			id:      2,
			parents: []nodeID{1},
			invoke: func(ctx context.Context, _ []any) (any, error) {
				close(started)
				<-ctx.Done()
				close(stopped)

				return nil, ctx.Err()
			},
		},
		{
			id:      3,
			parents: []nodeID{1},
			invoke: func(_ context.Context, _ []any) (any, error) {
				<-started
				panic("branch failed")
			},
		},
	}

	_, err := execute(t.Context(), plan, 3, nil, make(chan struct{}, 2))

	require.Error(t, err)
	require.ErrorContains(t, err, "branch failed")

	select {
	case <-stopped:
	default:
		require.FailNow(t, "execute returned before the sibling stopped")
	}
}

func TestExecute_CancellationWhileWaitingForGlobalSlot(t *testing.T) {
	t.Parallel()

	slots := make(chan struct{}, 1)
	slots <- struct{}{}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	plan := []node{{
		id:      1,
		parents: []nodeID{},
		invoke: func(_ context.Context, _ []any) (any, error) {
			require.FailNow(t, "step ran without a slot")

			return nil, assert.AnError
		},
	}}

	_, err := execute(ctx, plan, 1, nil, slots)

	require.ErrorIs(t, err, context.Canceled)
}

func TestExecute_WaitsForCanceledSiblings(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	started := make(chan struct{})
	stopped := make(chan struct{})
	plan := []node{
		{
			id:      1,
			parents: []nodeID{},
			invoke: func(_ context.Context, _ []any) (any, error) {
				return 1, nil
			},
		},
		{
			id:      2,
			parents: []nodeID{1},
			invoke: func(ctx context.Context, _ []any) (any, error) {
				close(started)
				<-ctx.Done()
				close(stopped)

				return nil, ctx.Err()
			},
		},
		{
			id:      3,
			parents: []nodeID{1},
			invoke: func(ctx context.Context, _ []any) (any, error) {
				select {
				case <-started:
					return nil, assert.AnError
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		},
	}

	_, err := execute(ctx, plan, 3, 0, make(chan struct{}, 2))

	require.ErrorIs(t, err, assert.AnError)

	select {
	case <-stopped:
	default:
		require.FailNow(t, "execute returned before the canceled sibling stopped")
	}
}

func receiveSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	select {
	case <-signal:
	case <-timer.C:
		require.FailNow(t, message)
	}
}

func updatePeak(peak *atomic.Int32, current int32) {
	for {
		observed := peak.Load()
		if current <= observed || peak.CompareAndSwap(observed, current) {
			return
		}
	}
}
