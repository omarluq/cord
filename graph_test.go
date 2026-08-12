package cord_test

import (
	"context"
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflow_RunExcludesUnreachableBranches(t *testing.T) {
	t.Parallel()

	var unreachableCalled bool

	runtime := cord.New()
	root := runtime.From("branches", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	selected := root.Then(func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	})
	_ = root.Then(func(_ context.Context, value int) (int, error) {
		unreachableCalled = true

		return value, nil
	})

	result, err := selected.Run(t.Context(), 4)

	require.NoError(t, err)
	assert.Equal(t, 5, result)
	assert.False(t, unreachableCalled)
}
