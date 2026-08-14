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
	root := runtime.From(passThrough)
	flow := cord.Join(root.Then(addOne), root.Then(timesTwo)).Then(sum)
	result, err := flow.Run(t.Context(), 3)
	require.NoError(t, err)
	require.Equal(t, 10, result)

	rows, err := database.QueryContext(t.Context(), `SELECT started_at, completed_at
		FROM cord_nodes ORDER BY remaining_deps, node_id`)

	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var (
		parentCompletions []string
		joinedStart       string
	)

	for rows.Next() {
		var startedAt, completedAt string
		require.NoError(t, rows.Scan(&startedAt, &completedAt))

		if len(parentCompletions) < 3 {
			parentCompletions = append(parentCompletions, completedAt)
		} else {
			joinedStart = startedAt
		}
	}

	require.NoError(t, rows.Err())
	require.Len(t, parentCompletions, 3)
	require.NotEmpty(t, joinedStart)

	for _, completedAt := range parentCompletions {
		require.LessOrEqual(t, completedAt, joinedStart)
	}
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
