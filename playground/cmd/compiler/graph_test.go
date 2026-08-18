package main

import (
	"testing"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/require"
)

const (
	incrementStep = "increment"
	doubleStep    = "double"
	loadStep      = "load"
	addThreeStep  = "addThree"
	sumStep       = "sum"
)

func TestExtractGraph(t *testing.T) {
	t.Parallel()

	graph, err := extractGraph(`package main
func main() {
    workflow := runtime.
        From("example", increment).
        Then(double)
    _, _ = workflow.Run(ctx, 4)
}`)
	require.NoError(t, err)
	require.Equal(t, protocol.Graph{
		Nodes: []protocol.Node{
			{ID: incrementStep, Label: incrementStep},
			{ID: doubleStep, Label: doubleStep},
		},
		Edges: []protocol.Edge{{From: incrementStep, To: doubleStep}},
	}, graph)
}

func TestExtractGraphWithVariablesAndJoin(t *testing.T) {
	t.Parallel()

	graph, err := extractGraph(`package main
func main() {
    root := runtime.From("example", load)
    left := root.Then(double)
    right := root.Then(addThree)
    workflow := cord.Join(left, right).Then(sum)
    _, _ = workflow.Run(ctx, 4)
}`)
	require.NoError(t, err)
	require.Equal(t, protocol.Graph{
		Nodes: []protocol.Node{
			{ID: loadStep, Label: loadStep},
			{ID: doubleStep, Label: doubleStep},
			{ID: addThreeStep, Label: addThreeStep},
			{ID: sumStep, Label: sumStep},
		},
		Edges: []protocol.Edge{
			{From: loadStep, To: doubleStep},
			{From: loadStep, To: addThreeStep},
			{From: doubleStep, To: sumStep},
			{From: addThreeStep, To: sumStep},
		},
	}, graph)
}

func TestInstrumentWorkflowEmitsStatesAroundStepExecution(t *testing.T) {
	t.Parallel()

	source := `package main
import "context"
func increment(_ context.Context, value int) (int, error) {
    return value + 1, nil
}
func main() { runtime.From("example", increment) }
`
	graph, err := extractGraph(source)
	require.NoError(t, err)

	instrumented, err := instrumentWorkflow(source, graph)
	require.NoError(t, err)
	require.Contains(t, instrumented, `func increment(_ context.Context, value int) (__cordResult0 int, __cordResult1 error)`)
	require.Contains(t, instrumented, `println("__CORD_NODE__:increment:running")`)
	require.Contains(t, instrumented, `println("__CORD_NODE__:increment:failed")`)
	require.Contains(t, instrumented, `println("__CORD_NODE__:increment:completed")`)
}

func TestInstrumentWorkflowPreservesNamedResults(t *testing.T) {
	t.Parallel()

	source := `package main
import "context"
func validate(_ context.Context, value int) (result int, err error) {
    return value, err
}
func main() { runtime.From("example", validate) }
`
	graph, err := extractGraph(source)
	require.NoError(t, err)

	instrumented, err := instrumentWorkflow(source, graph)
	require.NoError(t, err)
	require.Contains(t, instrumented, `func validate(_ context.Context, value int) (result int, err error)`)
	require.Contains(t, instrumented, `if err != nil`)
}
