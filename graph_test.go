package cord_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflow_RunExcludesUnreachableBranches(t *testing.T) {
	t.Parallel()

	database, runtime := newRuntime(t)
	root := runtime.From("test-workflow", passThrough)
	selected := root.Then(addOne)
	_ = root.Then(timesTwo)

	result, err := selected.Run(t.Context(), 4)
	require.NoError(t, err)
	assert.Equal(t, 5, result)

	var nodeCount int
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_nodes").Scan(&nodeCount))
	assert.Equal(t, 2, nodeCount)
}
