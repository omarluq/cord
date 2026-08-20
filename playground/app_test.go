package playground

import (
	"net/url"
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

func TestCompilerEndpoint(t *testing.T) {
	t.Parallel()

	pageURL, err := url.Parse("https://example.com/cord/")
	require.NoError(t, err)

	const sameOriginCompiler = "https://example.com/compile"

	tests := []struct {
		name     string
		endpoint string
		want     string
		allowed  bool
	}{
		{name: "relative", endpoint: "/compile", want: sameOriginCompiler, allowed: true},
		{name: "same origin", endpoint: sameOriginCompiler, want: sameOriginCompiler, allowed: true},
		{name: "different origin", endpoint: "https://evil.example/compile", want: "", allowed: false},
		{name: "credentials", endpoint: "https://user@example.com/compile", want: "", allowed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, allowed := compilerEndpoint(pageURL, test.endpoint, "")
			assert.Equal(t, test.allowed, allowed)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestCompilerEndpointAllowsDevelopmentCompiler(t *testing.T) {
	t.Parallel()

	pageURL, err := url.Parse("http://127.0.0.1:4173/")
	require.NoError(t, err)

	endpoint, allowed := compilerEndpoint(pageURL, defaultCompilerURL, "")
	require.True(t, allowed)
	assert.Equal(t, defaultCompilerURL, endpoint)
}

// TestCompilerEndpointAllowsConfiguredHTTPSCompiler verifies configured endpoint selection.
func TestCompilerEndpointAllowsConfiguredHTTPSCompiler(t *testing.T) {
	t.Parallel()

	pageURL, err := url.Parse("https://omarluq.github.io/cord/")
	require.NoError(t, err)

	const configured = "https://cord-compiler.run.app/compile"

	tests := []struct {
		name       string
		endpoint   string
		configured string
		want       string
		allowed    bool
	}{
		{name: "configured default", endpoint: "", configured: configured, want: configured, allowed: true},
		{name: "configured query", endpoint: configured, configured: configured, want: configured, allowed: true},
		{
			name: "arbitrary query", endpoint: "https://evil.example/compile",
			configured: configured, want: "", allowed: false,
		},
		{
			name: "insecure configured", endpoint: "",
			configured: "http://compiler.example/compile", want: "", allowed: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, allowed := compilerEndpoint(pageURL, test.endpoint, test.configured)
			assert.Equal(t, test.allowed, allowed)
			assert.Equal(t, test.want, got)
		})
	}
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
