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

// RetryPolicy controls persistent node retry scheduling.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: defaultMaxAttempts, BaseDelay: defaultBaseDelay, MaxDelay: defaultMaxDelay}
}

func (policy RetryPolicy) validate() error {
	if policy.MaxAttempts < 1 {
		return errors.New("cord: retry policy maximum attempts must be positive")
	}

	if policy.BaseDelay <= 0 {
		return errors.New("cord: retry policy base delay must be positive")
	}

	if policy.MaxDelay <= 0 || policy.MaxDelay < policy.BaseDelay {
		return errors.New("cord: retry policy maximum delay must be at least the base delay")
	}

	return nil
}

func retryDelay(policy RetryPolicy, attempt int) time.Duration {
	return backoff.FullJitter(policy.BaseDelay, policy.MaxDelay, attempt)
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
