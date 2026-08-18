package playground

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeOutputPathRejectsDangerousDirectories(t *testing.T) {
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

			_, err := safeOutputPath(test.path)
			assert.Error(t, err)
		})
	}
}

func TestSafeOutputPathAcceptsSeparateDirectory(t *testing.T) {
	t.Parallel()

	output := filepath.Join(t.TempDir(), "site")
	got, err := safeOutputPath(output)
	require.NoError(t, err)
	assert.Equal(t, output, got)
}
