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

	database, runtime := newRuntime(t)
	root := runtime.From("test-workflow", passThrough)
	flow := cord.Join(root.Then(addOne), root.Then(timesTwo)).Then(sum)
	result, err := flow.Run(t.Context(), 3)
	require.NoError(t, err)
	require.Equal(t, 10, result)

	rows, err := database.QueryContext(t.Context(), `SELECT parent.completed_at, child.started_at
		FROM cord_edges AS edge
		JOIN cord_nodes AS parent
			ON parent.run_id = edge.run_id AND parent.node_id = edge.parent_node_id
		JOIN cord_nodes AS child
			ON child.run_id = edge.run_id AND child.node_id = edge.child_node_id
		ORDER BY edge.child_node_id, edge.parent_order`)

	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var parentCompletions []string

	for rows.Next() {
		var parentCompleted, joinedStart string
		require.NoError(t, rows.Scan(&parentCompleted, &joinedStart))
		require.NotEmpty(t, parentCompleted)
		require.NotEmpty(t, joinedStart)
		require.LessOrEqual(t, parentCompleted, joinedStart)
		parentCompletions = append(parentCompletions, parentCompleted)
	}

	require.NoError(t, rows.Err())
	require.Len(t, parentCompletions, 4)
}

func TestWorkflow_PanicPersistsErrorMessage(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("test-workflow", panicWithError)
	_, err := flow.Run(t.Context(), 0)
	require.EqualError(t, err, "cord: workflow step panicked: panic cause")
}

func TestWorkflow_RejectsClosureBeforeExecution(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("test-workflow", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	_, err := flow.Run(t.Context(), 1)
	require.ErrorContains(t, err, "not a named package-level function")
}
