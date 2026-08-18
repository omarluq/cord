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

	tests := []struct {
		name     string
		filename string
		source   string
		found    bool
	}{
		{name: "present", filename: defaultExample, source: linearSource, found: true},
		{name: "missing", filename: "missing.go", source: "", found: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source, found := exampleSource(test.filename)
			require.Equal(t, test.source, source)
			require.Equal(t, test.found, found)
		})
	}
}
