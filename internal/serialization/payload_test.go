package serialization_test

import (
	"testing"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/stretchr/testify/assert"
)

func TestDiagnosePayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    int
		isLarge bool
	}{
		{name: "below threshold", size: serialization.PayloadWarningThreshold - 1, isLarge: false},
		{name: "at threshold", size: serialization.PayloadWarningThreshold, isLarge: false},
		{name: "above threshold", size: serialization.PayloadWarningThreshold + 1, isLarge: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			payload := make([]byte, test.size)
			diagnostic, isLarge := serialization.DiagnosePayload(payload)

			assert.Equal(t, test.isLarge, isLarge)
			assert.Equal(t, len(payload), diagnostic.Size)
			assert.Equal(t, serialization.PayloadWarningThreshold, diagnostic.Threshold)
			assert.Len(t, payload, test.size)
		})
	}
}
