package cord_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflow_DependenciesCompleteBeforeNodeStarts(t *testing.T) {
	t.Parallel()

	const (
		seed   = uint8(0x53)
		trials = 64
	)

	random := propertyRandom(seed)
	for trial := range trials {
		nodeCount := 2 + random.next(19)
		states := make([]atomic.Bool, int(nodeCount))
		workflows := make([]cord.Workflow[int, int], int(nodeCount))
		workflows[0] = cord.New().From("property", func(_ context.Context, value int) (int, error) {
			states[0].Store(true)

			return value, nil
		})

		for identifier := uint8(1); identifier < nodeCount; identifier++ {
			left := random.next(identifier)
			right := random.next(identifier)
			current := identifier

			workflows[int(current)] = cord.Join(workflows[int(left)], workflows[int(right)]).Then(
				func(_ context.Context, leftValue, rightValue int) (int, error) {
					if !states[int(left)].Load() || !states[int(right)].Load() {
						return 0, errors.New("node started before a dependency completed")
					}

					runtime.Gosched()
					states[int(current)].Store(true)

					return leftValue + rightValue, nil
				},
			)
		}

		_, err := workflows[int(nodeCount)-1].Run(t.Context(), 1)
		require.NoError(t, err, "seed=%x, trial=%d", seed, trial)
	}
}

func TestWorkflow_RuntimeBoundsConcurrencyAcrossRuns(t *testing.T) {
	t.Parallel()

	concurrency := max(1, runtime.GOMAXPROCS(0))
	runCount := concurrency + 1

	var (
		active atomic.Int32
		peak   atomic.Int32
	)

	started := make(chan struct{}, runCount)
	release := make(chan struct{})
	flow := cord.New().From("bounded", func(ctx context.Context, value int) (int, error) {
		current := active.Add(1)
		defer active.Add(-1)

		updatePeak(&peak, current)

		started <- struct{}{}

		select {
		case <-release:
			return value, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	var waitGroup sync.WaitGroup
	for index := range runCount {
		waitGroup.Go(func() {
			_, err := flow.Run(t.Context(), index)
			assert.NoError(t, err)
		})
	}

	for range concurrency {
		receiveSignal(t, started, "waiting for concurrent workflow run to start")
	}

	assert.EqualValues(t, concurrency, active.Load())
	close(release)
	waitGroup.Wait()
	assert.EqualValues(t, concurrency, peak.Load())
}

func TestWorkflow_PanicPreservesErrorIdentity(t *testing.T) {
	t.Parallel()

	expected := errors.New("panic cause")
	flow := cord.New().From("panic", func(_ context.Context, _ int) (int, error) {
		panic(expected)
	})

	_, err := flow.Run(t.Context(), 0)

	require.ErrorIs(t, err, expected)
	require.ErrorContains(t, err, "workflow step panicked")
}

func TestWorkflow_WaitsForSiblingAfterPanic(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	stopped := make(chan struct{})
	root := cord.New().From("siblings", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	left := root.Then(func(ctx context.Context, value int) (int, error) {
		close(started)
		<-ctx.Done()
		close(stopped)

		return value, ctx.Err()
	})
	right := root.Then(func(_ context.Context, _ int) (int, error) {
		<-started
		panic("branch failed")
	})
	flow := cord.Join(left, right).Then(func(_ context.Context, leftValue, rightValue int) (int, error) {
		return leftValue + rightValue, nil
	})

	_, err := flow.Run(t.Context(), 1)

	require.ErrorContains(t, err, "branch failed")

	select {
	case <-stopped:
	default:
		require.FailNow(t, "workflow returned before the sibling stopped")
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

type propertyRandom uint8

func (random *propertyRandom) next(limit uint8) uint8 {
	value := uint8(*random)
	value ^= value << 3
	value ^= value >> 5
	value ^= value << 1
	*random = propertyRandom(value)

	return value % limit
}
