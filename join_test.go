package cord_test

import (
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Register the pure-Go SQLite driver used by the tests.
	_ "modernc.org/sqlite"
)

func TestWorkflow_RunJoinedBranches(t *testing.T) {
	t.Parallel()

	root := mustRuntime(t).From("test-workflow", timesTwo)
	flow := cord.Join(root.Then(leftText), root.Then(addOne)).Then(formatJoined)
	result, err := flow.Run(t.Context(), 2)
	require.NoError(t, err)
	assert.Equal(t, "left:5", result)
}

func TestJoin_UnrelatedWorkflowsFailAtRun(t *testing.T) {
	t.Parallel()
	runtime := mustRuntime(t)
	left := runtime.From("left-workflow", passThrough)
	right := runtime.From("right-workflow", passThrough)
	joined := cord.Join(left, right).Then(sum)
	result, err := joined.Run(t.Context(), 1)
	assert.Zero(t, result)
	require.EqualError(t, err, "cord: cannot join unrelated workflows")
}

func TestJoin_IdenticalTailAliasesFailBeforePersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		flow func(*cord.Cord) cord.Workflow[int, int]
		name string
	}{
		{
			name: "direct root",
			flow: func(runtime *cord.Cord) cord.Workflow[int, int] {
				root := runtime.From("direct-identical-tail", passThrough)

				return cord.Join(root, root).Then(sum)
			},
		},
		{
			name: "nested alias",
			flow: func(runtime *cord.Cord) cord.Workflow[int, int] {
				root := runtime.From("nested-identical-tail", passThrough)
				nested := root.Then(addOne).Then(timesTwo)
				alias := nested

				return cord.Join(nested, alias).Then(sum)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			database, runtime := newRuntime(t)
			result, err := testCase.flow(runtime).Run(t.Context(), 1)
			assert.Zero(t, result)
			require.EqualError(t, err, "cord: cannot join identical workflow tails")

			var runs int
			require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_runs").Scan(&runs))
			assert.Zero(t, runs)
		})
	}
}

func TestJoin_DistinctTailsUsingSameFunctionPreserveParentOrder(t *testing.T) {
	t.Parallel()

	root := mustRuntime(t).From("same-function-siblings", passThrough)
	left := root.Then(addOne).Then(timesTwo)
	right := root.Then(timesTwo).Then(timesTwo)

	result, err := cord.Join(left, right).Then(subtract).Run(t.Context(), 2)
	require.NoError(t, err)
	assert.Equal(t, -2, result)
}
