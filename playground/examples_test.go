package playground_test

import (
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExampleScriptsAreValidGoPrograms(t *testing.T) {
	t.Parallel()

	linear, err := os.ReadFile("examples/linear.go.txt")
	require.NoError(t, err)
	branchJoin, err := os.ReadFile("examples/branch_join.go.txt")
	require.NoError(t, err)
	retry, err := os.ReadFile("examples/retry.go.txt")
	require.NoError(t, err)
	largePipeline, err := os.ReadFile("examples/large_pipeline.go.txt")
	require.NoError(t, err)
	httpRequest, err := os.ReadFile("examples/http_request.go.txt")
	require.NoError(t, err)
	permanentFailure, err := os.ReadFile("examples/permanent_failure.go.txt")
	require.NoError(t, err)

	examples := map[string][]byte{
		"linear.go":            linear,
		"branch_join.go":       branchJoin,
		"retry.go":             retry,
		"large_pipeline.go":    largePipeline,
		"http_request.go":      httpRequest,
		"permanent_failure.go": permanentFailure,
	}

	for filename, source := range examples {
		t.Run(filename, func(t *testing.T) {
			t.Parallel()
			require.NotEmpty(t, source)

			_, err := parser.ParseFile(token.NewFileSet(), filename, source, parser.AllErrors)
			require.NoError(t, err)
		})
	}
}
