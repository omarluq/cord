package cord_test

import (
	"context"
	"errors"
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/require"
)

var errPanicCause = errors.New("panic cause")

func panicWithError(_ context.Context, _ int) (int, error) { panic(errPanicCause) }

func TestWorkflow_DependenciesCompleteBeforeNodeStarts(t *testing.T) {
	t.Parallel()

	root := mustRuntime(t).From(passThrough)
	flow := cord.Join(root.Then(addOne), root.Then(timesTwo)).Then(sum)
	result, err := flow.Run(t.Context(), 3)
	require.NoError(t, err)
	require.Equal(t, 10, result)
}

func TestWorkflow_PanicPreservesErrorIdentity(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From(panicWithError)
	_, err := flow.Run(t.Context(), 0)
	require.ErrorIs(t, err, errPanicCause)
	require.ErrorContains(t, err, "workflow step panicked")
}

func TestWorkflow_RejectsClosureBeforeExecution(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From(func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	_, err := flow.Run(t.Context(), 1)
	require.ErrorContains(t, err, "not a named package-level function")
}

var _ cord.Workflow[int, int]
