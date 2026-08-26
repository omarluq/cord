package playground_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExampleScriptsAreValidGoPrograms(t *testing.T) {
	t.Parallel()

	examples := os.DirFS("examples")
	discovered := 0

	err := fs.WalkDir(examples, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() {
			return nil
		}

		if !strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".go.txt") {
			return nil
		}

		discovered++

		t.Run(path, func(t *testing.T) {
			t.Parallel()

			source, readErr := fs.ReadFile(examples, path)
			require.NoError(t, readErr)
			require.NotEmpty(t, source)

			_, parseErr := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
			require.NoError(t, parseErr)
		})

		return nil
	})
	require.NoError(t, err)
	require.Positive(t, discovered)
}
