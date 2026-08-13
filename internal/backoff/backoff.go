// Package backoff calculates retry delays.
package backoff

import (
	"crypto/rand"
	"math/big"
	"time"
)

// FullJitter returns a random delay from zero through the exponential cap.
func FullJitter(baseDelay, maxDelay time.Duration, attempt int) time.Duration {
	capDelay := baseDelay
	for current := 1; current < attempt && capDelay < maxDelay; current++ {
		if capDelay > maxDelay/2 {
			capDelay = maxDelay

			break
		}

		capDelay *= 2
	}

	capDelay = min(capDelay, maxDelay)

	value, err := rand.Int(rand.Reader, big.NewInt(int64(capDelay)+1))
	if err != nil {
		return 0
	}

	return time.Duration(value.Int64())
}
