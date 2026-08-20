package cord_test

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncompatibleSignaturesFailToCompile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run      func(*testing.T) ([]byte, error)
		name     string
		expected []string
	}{
		{
			name:     "Then",
			expected: []string{"signature.go:15", "value string", "does not match func(context.Context, int)"},
			run: func(t *testing.T) ([]byte, error) {
				t.Helper()

				return exec.CommandContext(t.Context(), "go", "test", "./testdata/compilefail/then").CombinedOutput()
			},
		},
		{
			name:     "Join",
			expected: []string{"signature.go:18", "leftValue int", "does not match func(context.Context, string, int)"},
			run: func(t *testing.T) ([]byte, error) {
				t.Helper()

				return exec.CommandContext(t.Context(), "go", "test", "./testdata/compilefail/join").CombinedOutput()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			output, err := test.run(t)
			require.Error(t, err, "incompatible signature unexpectedly compiled:\n%s", output)

			diagnostic := string(output)
			for _, expected := range test.expected {
				assert.Contains(t, diagnostic, expected)
			}

			assert.NotContains(t, diagnostic, "not enough arguments in call to cord.New")
			assert.NotContains(t, diagnostic, "no such file or directory")
			assert.NotContains(t, diagnostic, "cannot find package")
		})
	}
}
