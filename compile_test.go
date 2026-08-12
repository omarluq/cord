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
		run  func(*testing.T) ([]byte, error)
		name string
	}{
		{
			name: "Then",
			run: func(t *testing.T) ([]byte, error) {
				t.Helper()

				return exec.CommandContext(t.Context(), "go", "test", "./testdata/compilefail/then").CombinedOutput()
			},
		},
		{
			name: "Join",
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
			assert.Contains(t, string(output), "signature.go")
		})
	}
}
