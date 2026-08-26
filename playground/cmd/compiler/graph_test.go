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

	tests := []struct {
		name   string
		source string
		graph  protocol.Graph
	}{
		{
			name: "linear chain",
			source: `package main
func main() {
    workflow := runtime.
        From("example", increment).
        Then(double)
    _, _ = workflow.Run(ctx, 4)
}`,
			graph: protocol.Graph{
				Nodes: []protocol.Node{
					{ID: incrementStep, Label: incrementStep},
					{ID: doubleStep, Label: doubleStep},
				},
				Edges: []protocol.Edge{{From: incrementStep, To: doubleStep}},
			},
		},
		{
			name: "variables and join",
			source: `package main
func main() {
    root := runtime.From("example", load)
    left := root.Then(double)
    right := root.Then(addThree)
    workflow := cord.Join(left, right).Then(sum)
    _, _ = workflow.Run(ctx, 4)
}`,
			graph: protocol.Graph{
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
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			graph, err := extractGraph(test.source)
			require.NoError(t, err)
			require.Equal(t, test.graph, graph)
		})
	}
}
