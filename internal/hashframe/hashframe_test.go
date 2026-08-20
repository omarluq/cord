package hashframe_test

import (
	"testing"

	"github.com/omarluq/cord/internal/hashframe"
	"github.com/stretchr/testify/assert"
)

func TestSHA256_GoldenFraming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		parts []string
	}{
		{
			parts: nil,
			name:  "empty",
			want:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			parts: []string{"cord"},
			name:  "single",
			want:  "fb9c1c90482dd6613558246de872288c3b4d26315b97f8eba87dafdd32495e5b",
		},
		{
			parts: []string{"a", "bc"},
			name:  "boundaries",
			want:  "5310a58788781ab25d5ad7c3f85035824b4eb7bdfa394e0ac2186271472b5492",
		},
		{
			parts: []string{"é"},
			name:  "unicode byte length",
			want:  "2f23c71856587c2a6ffdb64f0e11e9940b35da318947308f6a02ee01898e6107",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, hashframe.SHA256(test.parts...))
		})
	}
}
