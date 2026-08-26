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
		{name: "nonpositive attempt", attempt: 0, maximum: time.Millisecond},
		{name: "first", attempt: 1, maximum: time.Millisecond},
		{name: "second", attempt: 2, maximum: 2 * time.Millisecond},
		{name: "capped", attempt: 3, maximum: 4 * time.Millisecond},
		{name: "overflow protected", attempt: 20, maximum: 4 * time.Millisecond},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			observedNonzero := false

			for range 100 {
				delay := backoff.FullJitter(time.Millisecond, 4*time.Millisecond, testCase.attempt)
				assert.GreaterOrEqual(t, delay, time.Duration(0))
				assert.LessOrEqual(t, delay, testCase.maximum)
				observedNonzero = observedNonzero || delay > 0
			}

			assert.True(t, observedNonzero)
		})
	}
}

func TestFullJitterInvalidDelays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		base time.Duration
		max  time.Duration
	}{
		{name: "zero base", base: 0, max: time.Second},
		{name: "negative base", base: -time.Second, max: time.Second},
		{name: "zero maximum", base: time.Second, max: 0},
		{name: "negative maximum", base: time.Second, max: -time.Second},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assert.Zero(t, backoff.FullJitter(testCase.base, testCase.max, 1))
		})
	}
}
