package backoff_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/backoff"
	"github.com/stretchr/testify/assert"
)

func TestFullJitterBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		attempt int
		maximum time.Duration
	}{
		{name: "first", attempt: 1, maximum: time.Millisecond},
		{name: "second", attempt: 2, maximum: 2 * time.Millisecond},
		{name: "capped", attempt: 3, maximum: 4 * time.Millisecond},
		{name: "overflow protected", attempt: 20, maximum: 4 * time.Millisecond},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			for range 100 {
				delay := backoff.FullJitter(time.Millisecond, 4*time.Millisecond, testCase.attempt)
				assert.GreaterOrEqual(t, delay, time.Duration(0))
				assert.LessOrEqual(t, delay, testCase.maximum)
			}
		})
	}
}
