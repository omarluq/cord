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

func TestInstrumentWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		source   string
		contains []string
		excludes []string
	}{
		{
			name: "unnamed results",
			source: `package main
import "context"
func increment(_ context.Context, value int) (int, error) {
    return value + 1, nil
}
func main() { runtime.From("example", increment) }
`,
			contains: []string{
				`func increment(_ context.Context, value int) (__cordResult0 int, __cordResult1 error)`,
				`println("__CORD_NODE__:increment:running")`,
				`println("__CORD_NODE__:increment:failed")`,
				`println("__CORD_NODE__:increment:completed")`,
			},
			excludes: []string{},
		},
		{
			name: "named results",
			source: `package main
import "context"
func validate(_ context.Context, value int) (result int, err error) {
    return value, err
}
func main() { runtime.From("example", validate) }
`,
			contains: []string{
				`func validate(_ context.Context, value int) (result int, err error)`,
				`if err != nil`,
			},
			excludes: []string{},
		},
		{
			name: "non-error second result",
			source: `package main
import "context"
func pair(_ context.Context, value int) (int, int) { return value, value }
func main() { runtime.From("example", pair) }
`,
			contains: []string{},
			excludes: []string{"__CORD_NODE__"},
		},
		{
			name:     "no workflow nodes",
			source:   "package main\nfunc main() {}\n",
			contains: []string{"package main\nfunc main() {}\n"},
			excludes: []string{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			graph, err := extractGraph(test.source)
			require.NoError(t, err)
			instrumented, err := instrumentWorkflow(test.source, graph)
			require.NoError(t, err)

			for _, expected := range test.contains {
				require.Contains(t, instrumented, expected)
			}

			for _, unexpected := range test.excludes {
				require.NotContains(t, instrumented, unexpected)
			}
		})
	}
}

func TestInstrumentWorkflowSkipsAmbiguousStepNames(t *testing.T) {
	t.Parallel()

	source := `package main
import "context"
func load(_ context.Context, value int) (int, error) { return value, nil }
`
	graph := protocol.Graph{
		Nodes: []protocol.Node{
			{ID: "pkga.load", Label: "pkga.load"},
			{ID: "pkgb.load", Label: "pkgb.load"},
		},
		Edges: []protocol.Edge{},
	}
	instrumented, err := instrumentWorkflow(source, graph)
	require.NoError(t, err)
	require.NotContains(t, instrumented, "__CORD_NODE__")
}
