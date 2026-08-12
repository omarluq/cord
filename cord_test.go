package cord_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNew_CreatesRuntime(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, cord.New())
}

func TestWorkflow_RunLinearChain(t *testing.T) {
	t.Parallel()

	runtime := cord.New()
	flow := runtime.From("math", func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	}).Then(func(_ context.Context, value int) (bool, error) {
		return value == 3, nil
	})

	result, err := flow.Run(t.Context(), 2)

	require.NoError(t, err)
	assert.True(t, result)
}

func TestWorkflow_RunJoinedBranches(t *testing.T) {
	t.Parallel()

	runtime := cord.New()
	root := runtime.From("release", func(_ context.Context, value int) (int, error) {
		return value * 2, nil
	})
	left := root.Then(func(_ context.Context, _ int) (string, error) {
		return "left", nil
	})
	right := root.Then(func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	})
	joined := cord.Join(left, right).Then(func(_ context.Context, leftValue string, rightValue int) (string, error) {
		return fmt.Sprintf("%s:%d", leftValue, rightValue), nil
	})

	result, err := joined.Run(t.Context(), 2)

	require.NoError(t, err)
	assert.Equal(t, "left:5", result)
}

func TestWorkflow_RunExcludesUnreachableBranch(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	runtime := cord.New()
	root := runtime.From("branches", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	selected := root.Then(func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	})
	_ = root.Then(func(_ context.Context, value int) (int, error) {
		calls.Add(1)

		return value, nil
	})

	result, err := selected.Run(t.Context(), 4)

	require.NoError(t, err)
	assert.Equal(t, 5, result)
	assert.Zero(t, calls.Load())
}

func TestWorkflow_RunPropagatesNodeError(t *testing.T) {
	t.Parallel()

	expected := errors.New("step failed")

	var descendantCalls atomic.Int32

	runtime := cord.New()
	flow := runtime.From("failure", func(_ context.Context, value int) (int, error) {
		return value, expected
	}).Then(func(_ context.Context, value int) (int, error) {
		descendantCalls.Add(1)

		return value, nil
	})

	result, err := flow.Run(t.Context(), 1)

	assert.Zero(t, result)
	require.ErrorIs(t, err, expected)
	assert.Zero(t, descendantCalls.Load())
}

func TestJoin_UnrelatedWorkflowsFailAtRun(t *testing.T) {
	t.Parallel()

	runtime := cord.New()
	left := runtime.From("left", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	right := runtime.From("right", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	joined := cord.Join(left, right).Then(func(_ context.Context, leftValue, rightValue int) (int, error) {
		return leftValue + rightValue, nil
	})

	result, err := joined.Run(t.Context(), 1)

	assert.Zero(t, result)
	require.EqualError(t, err, "cord: cannot join unrelated workflows")
}

func TestWorkflow_NilStepsFailAtRun(t *testing.T) {
	t.Parallel()

	runtime := cord.New()

	var rootStep func(context.Context, int) (int, error)

	rootResult, rootErr := runtime.From("nil-root", rootStep).Run(t.Context(), 1)

	assert.Zero(t, rootResult)
	require.EqualError(t, rootErr, "cord: root step is nil")

	root := runtime.From("nil-then", func(_ context.Context, value int) (int, error) {
		return value, nil
	})

	var nextStep func(context.Context, int) (string, error)

	nextResult, nextErr := root.Then(nextStep).Run(t.Context(), 1)

	assert.Empty(t, nextResult)
	require.EqualError(t, nextErr, "cord: workflow step is nil")

	var joinStep func(context.Context, int, int) (int, error)

	joinResult, joinErr := cord.Join(root, root).Then(joinStep).Run(t.Context(), 1)

	assert.Zero(t, joinResult)
	require.EqualError(t, joinErr, "cord: joined workflow step is nil")
}

func TestJoin_ReturnsExportedResult(t *testing.T) {
	t.Parallel()

	runtime := cord.New()
	root := runtime.From("join-result", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	joined := cord.Join(root, root)

	var _ = joined

	flow := joined.Then(func(_ context.Context, left, right int) (int, error) {
		return left + right, nil
	})
	result, err := flow.Run(t.Context(), 2)

	require.NoError(t, err)
	assert.Equal(t, 4, result)
}

func TestWorkflow_RunPreservesNilInterfaceOutputs(t *testing.T) {
	t.Parallel()

	runtime := cord.New()
	root := runtime.From("nil-interface", func(_ context.Context, _ int) (any, error) {
		return nil, nil
	})
	flow := root.Then(func(_ context.Context, value any) (any, error) {
		assert.Nil(t, value)

		return nil, nil
	})
	joined := cord.Join(root, flow).Then(func(_ context.Context, left, right any) (any, error) {
		assert.Nil(t, left)
		assert.Nil(t, right)

		return nil, nil
	})

	result, err := joined.Run(t.Context(), 1)

	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestWorkflow_RunPreservesNilCapableOutputs(t *testing.T) {
	t.Parallel()

	t.Run("pointer", func(t *testing.T) {
		t.Parallel()

		flow := cord.New().From("nil-pointer", func(_ context.Context, _ int) (*int, error) {
			return nil, nil
		})
		result, err := flow.Run(t.Context(), 1)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("map", func(t *testing.T) {
		t.Parallel()

		flow := cord.New().From("nil-map", func(_ context.Context, _ int) (map[string]int, error) {
			return nil, nil
		})
		result, err := flow.Run(t.Context(), 1)

		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("slice", func(t *testing.T) {
		t.Parallel()

		flow := cord.New().From("nil-slice", func(_ context.Context, _ int) ([]int, error) {
			return nil, nil
		})
		result, err := flow.Run(t.Context(), 1)

		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestWorkflow_RunAcceptsNilInterfaceInput(t *testing.T) {
	t.Parallel()

	flow := cord.New().From("nil-input", func(_ context.Context, value any) (bool, error) {
		return value == nil, nil
	})

	result, err := flow.Run(t.Context(), nil)

	require.NoError(t, err)
	assert.True(t, result)
}

func TestWorkflow_RunConvertsPanicToError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		panicValue any
		want       string
	}{
		{name: "string", panicValue: "broken step", want: "broken step"},
		{name: "error", panicValue: assert.AnError, want: assert.AnError.Error()},
		{name: "nil", panicValue: nil, want: "workflow step panicked"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			runtime := cord.New()
			flow := runtime.From("panic", func(_ context.Context, _ int) (int, error) {
				panic(test.panicValue)
			})

			result, err := flow.Run(t.Context(), 1)

			assert.Zero(t, result)
			require.Error(t, err)
			require.ErrorContains(t, err, test.want)

			if panicErr, ok := test.panicValue.(error); ok {
				assert.ErrorIs(t, err, panicErr)
			}
		})
	}
}

func TestWorkflow_RunWithCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var calls atomic.Int32

	runtime := cord.New()
	flow := runtime.From("canceled", func(_ context.Context, value int) (int, error) {
		calls.Add(1)

		return value, nil
	})

	result, err := flow.Run(ctx, 1)

	assert.Zero(t, result)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, calls.Load())
}

func TestWorkflow_RunCancellationDuringExecution(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	runtime := cord.New()
	flow := runtime.From("cancel-during-run", func(ctx context.Context, _ int) (int, error) {
		close(started)
		<-ctx.Done()

		return 0, ctx.Err()
	})

	done := make(chan error, 1)

	go func() {
		_, err := flow.Run(ctx, 1)
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		require.FailNow(t, "workflow step did not start")
	}

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		require.FailNow(t, "workflow did not stop after cancellation")
	}
}

func TestWorkflow_ConcurrentConstructionAndReuse(t *testing.T) {
	t.Parallel()

	runtime := cord.New()
	root := runtime.From("concurrent", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	shared := root.Then(func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	})

	const goroutines = 16

	var waitGroup sync.WaitGroup
	for index := range goroutines {
		waitGroup.Go(func() {
			flow := root.Then(func(_ context.Context, value int) (int, error) {
				return value + 1, nil
			})
			result, err := flow.Run(t.Context(), index)
			require.NoError(t, err)
			assert.Equal(t, index+1, result)

			sharedResult, sharedErr := shared.Run(t.Context(), index)
			require.NoError(t, sharedErr)
			assert.Equal(t, index+1, sharedResult)
		})
	}

	waitGroup.Wait()
}

func TestWorkflow_ZeroValueFailsWithoutPanic(t *testing.T) {
	t.Parallel()

	var flow cord.Workflow[int, int]

	chained := flow.Then(func(_ context.Context, value int) (int, error) {
		return value, nil
	})

	result, err := chained.Run(t.Context(), 1)

	assert.Zero(t, result)
	require.EqualError(t, err, "cord: invalid workflow")
}
