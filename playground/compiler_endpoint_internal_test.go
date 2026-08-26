package playground

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
