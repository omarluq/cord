package cord_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflow_RunExcludesUnreachableBranches(t *testing.T) {
	t.Parallel()

	root := mustRuntime(t).From(passThrough)
	selected := root.Then(addOne)
	_ = root.Then(timesTwo)

	result, err := selected.Run(t.Context(), 4)
	require.NoError(t, err)
	assert.Equal(t, 5, result)
}
