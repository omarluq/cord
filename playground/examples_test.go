package playground_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExampleScriptsAreValidGoPrograms(t *testing.T) {
	t.Parallel()

	examples := os.DirFS("examples")
	entries, err := fs.ReadDir(examples, ".")
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		t.Run(filename, func(t *testing.T) {
			t.Parallel()

			source, readErr := fs.ReadFile(examples, filename)
			require.NoError(t, readErr)
			require.NotEmpty(t, source)

			_, parseErr := parser.ParseFile(token.NewFileSet(), filename, source, parser.AllErrors)
			require.NoError(t, parseErr)
		})
	}
}
