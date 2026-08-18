package playground

import (
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExampleScriptsAreValidGoPrograms(t *testing.T) {
	t.Parallel()

	require.Len(t, exampleScripts, 6)
	for _, script := range exampleScripts {
		t.Run(script.filename, func(t *testing.T) {
			t.Parallel()
			require.NotEmpty(t, script.source)
			_, err := parser.ParseFile(token.NewFileSet(), script.filename, script.source, parser.AllErrors)
			require.NoError(t, err)
		})
	}
}

func TestExampleSource(t *testing.T) {
	t.Parallel()

	source, ok := exampleSource(defaultExample)
	require.True(t, ok)
	require.Equal(t, linearSource, source)

	_, ok = exampleSource("missing.go")
	require.False(t, ok)
}
