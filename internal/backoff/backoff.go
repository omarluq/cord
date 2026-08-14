// Package backoff calculates retry delays.
package backoff

import (
	"crypto/rand"
	"io"
	"math/big"
	"time"
)

// FullJitter returns a random delay from zero through the exponential cap.
func FullJitter(baseDelay, maxDelay time.Duration, attempt int) time.Duration {
	return fullJitter(rand.Reader, baseDelay, maxDelay, attempt)
}

func fullJitter(random io.Reader, baseDelay, maxDelay time.Duration, attempt int) time.Duration {
	if baseDelay <= 0 || maxDelay <= 0 {
		return 0
	}

	capDelay := baseDelay
	for current := 1; current < attempt && capDelay < maxDelay; current++ {
		if capDelay > maxDelay/2 {
			capDelay = maxDelay

			break
		}

		capDelay *= 2
	}

	capDelay = min(capDelay, maxDelay)
	upperBound := new(big.Int).Add(big.NewInt(int64(capDelay)), big.NewInt(1))

	value, err := rand.Int(random, upperBound)
	if err != nil {
		return capDelay
	}

	return time.Duration(value.Int64())
}
