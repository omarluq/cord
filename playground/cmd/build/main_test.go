package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateCompilerURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "empty", value: "", wantErr: false},
		{name: "Cloud Run", value: "https://compiler-123.run.app/compile", wantErr: false},
		{name: "HTTP", value: "http://compiler.example/compile", wantErr: true},
		{name: "wrong path", value: "https://compiler.example/other", wantErr: true},
		{name: "credentials", value: "https://user@compiler.example/compile", wantErr: true},
		{name: "query", value: "https://compiler.example/compile?x=1", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateCompilerURL(test.value)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRunGeneratesStaticWebsite(t *testing.T) {
	t.Parallel()

	output := filepath.Join(t.TempDir(), "site")
	require.NoError(t, run([]string{"-output", output, "-prefix", "/cord"}))

	for _, path := range []string{
		"index.html",
		"app.js",
		"app-worker.js",
		"wasm_exec.js",
		"manifest.webmanifest",
		filepath.Join("web", "images", "icon.svg"),
		filepath.Join("web", "images", "icon.png"),
		filepath.Join("web", "images", "go-logo.svg"),
	} {
		info, err := os.Stat(filepath.Join(output, path))
		require.NoError(t, err, path)
		require.False(t, info.IsDir(), path)
	}

	indexFile, err := os.DirFS(output).Open("index.html")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, indexFile.Close()) })

	index, err := io.ReadAll(indexFile)
	require.NoError(t, err)
	require.Contains(t, string(index), `href="/cord/manifest.webmanifest"`)
	require.Contains(t, string(index), `src="/cord/app.js"`)
	require.Contains(t, string(index), `href="/cord/web/playground.css"`)
}
