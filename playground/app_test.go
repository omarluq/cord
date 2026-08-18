package playground

import (
	"testing"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompilationCache(t *testing.T) {
	t.Parallel()

	artifact := compilationArtifact{
		graph: protocol.Graph{Nodes: []protocol.Node{{ID: "step", Label: "step"}}, Edges: []protocol.Edge{}},
		wasm:  []byte("wasm"),
	}
	var cache compilationCache

	_, found := cache.get("source")
	assert.False(t, found)

	cache.put("source", artifact)
	cached, found := cache.get("source")
	require.True(t, found)
	assert.Equal(t, artifact, cached)

	_, found = cache.get("changed source")
	assert.False(t, found)
}

func TestAppendOutput(t *testing.T) {
	t.Parallel()

	const firstLine = "first"
	tests := []struct {
		name    string
		current string
		next    string
		want    string
	}{
		{name: "first line", current: "", next: firstLine, want: firstLine},
		{name: "replace running placeholder", current: "Running workflow…", next: firstLine, want: firstLine},
		{name: "append line", current: firstLine, next: "second", want: "first\nsecond"},
		{name: "avoid blank line", current: "first\n", next: "second", want: "first\nsecond"},
		{name: "ignore empty line", current: firstLine, next: "", want: firstLine},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, appendOutput(test.current, test.next))
		})
	}
}
