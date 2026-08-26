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
