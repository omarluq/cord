package playground_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/omarluq/cord/playground"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateStaticRejectsDangerousDirectories(t *testing.T) {
	t.Parallel()

	workingDirectory, err := os.Getwd()
	require.NoError(t, err)

	parent := filepath.Dir(workingDirectory)
	root := filepath.VolumeName(workingDirectory) + string(filepath.Separator)

	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "current directory", path: "."},
		{name: "working directory", path: workingDirectory},
		{name: "parent", path: parent},
		{name: "root", path: root},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := playground.GenerateStatic(test.path, "")
			assert.Error(t, err)
		})
	}
}
