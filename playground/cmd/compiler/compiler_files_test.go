package main

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimitedBuffer(t *testing.T) {
	t.Parallel()

	buffer := newLimitedBuffer(4)
	written, err := buffer.Write([]byte("abcdef"))
	require.NoError(t, err)
	require.Equal(t, 6, written)
	require.Equal(t, "abcd\n[compiler diagnostics truncated]", buffer.String())
}

func TestReadWASMArtifactSizeBoundary(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	require.NoError(t, os.WriteFile(directory+"/app.wasm", []byte("wasm"), 0o600))

	tests := []struct {
		name      string
		wantError string
		limit     int64
	}{
		{name: "oversized", wantError: "WebAssembly exceeds 3-byte limit", limit: 3},
		{name: "exact limit", wantError: "", limit: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wasm, err := readWASM(directory, test.limit)
			if test.wantError != "" {
				require.EqualError(t, err, test.wantError)

				return
			}

			require.NoError(t, err)
			require.Equal(t, []byte("wasm"), wasm)
		})
	}
}

func TestModuleSourceQuotesCordDirectory(t *testing.T) {
	t.Parallel()

	const cordDirectory = "/tmp/cord workspace"

	require.Contains(
		t,
		moduleSource(cordDirectory),
		"replace "+cordModule+" => "+strconv.Quote(cordDirectory),
	)
}
