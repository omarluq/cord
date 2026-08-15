package cord

import (
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/backoff"
)

const (
	defaultMaxAttempts = 3
	defaultBaseDelay   = 500 * time.Millisecond
	defaultMaxDelay    = 30 * time.Second
	retryPolicyVersion = 1
)

type retryPolicy struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

func defaultRetryPolicy() retryPolicy {
	return retryPolicy{maxAttempts: defaultMaxAttempts, baseDelay: defaultBaseDelay, maxDelay: defaultMaxDelay}
}

func (policy retryPolicy) validate() error {
	if policy.maxAttempts < 1 {
		return errors.New("cord: retry policy maximum attempts must be positive")
	}

	if policy.baseDelay <= 0 {
		return errors.New("cord: retry policy base delay must be positive")
	}

	if policy.maxDelay <= 0 || policy.maxDelay < policy.baseDelay {
		return errors.New("cord: retry policy maximum delay must be at least the base delay")
	}

	return nil
}

func retryDelay(policy retryPolicy, attempt int) time.Duration {
	return backoff.FullJitter(policy.baseDelay, policy.maxDelay, attempt)
}

type permanentError struct{ err error }

func (err permanentError) Error() string { return fmt.Sprintf("%v", err.err) }
func (err permanentError) Unwrap() error { return err.err }

// Permanent marks err as terminal so Cord skips remaining retry attempts.
// A nil error remains nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return permanentError{err: err}
}

func isPermanent(err error) bool {
	var marker permanentError

	return errors.As(err, &marker)
}
