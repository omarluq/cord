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

	const containsWorkingDirectory = "contains the working directory"

	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{name: "empty", path: "", wantErr: "static output path is empty"},
		{name: "current directory", path: ".", wantErr: containsWorkingDirectory},
		{name: "working directory", path: workingDirectory, wantErr: containsWorkingDirectory},
		{name: "parent", path: parent, wantErr: containsWorkingDirectory},
		{name: "root", path: root, wantErr: "is a filesystem root"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := playground.GenerateStatic(test.path, "")
			require.Error(t, err)
			assert.ErrorContains(t, err, test.wantErr)
		})
	}
}
