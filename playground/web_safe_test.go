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

	for _, path := range []string{"", ".", workingDirectory, parent, root} {
		_, err := safeOutputPath(path)
		assert.Error(t, err, path)
	}
}

func TestSafeOutputPathAcceptsSeparateDirectory(t *testing.T) {
	t.Parallel()

	output := filepath.Join(t.TempDir(), "site")
	got, err := safeOutputPath(output)
	require.NoError(t, err)
	assert.Equal(t, output, got)
}
