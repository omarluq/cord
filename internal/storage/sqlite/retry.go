package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/backoff"
)

const retryAttempts = 20

func retryContention(ctx context.Context, operation string, operationFunc func(context.Context) error) error {
	return retry(ctx, operation, time.Time{}, isBusy, operationFunc)
}

func retryFencedContention(
	ctx context.Context,
	operation string,
	leaseExpiresAt time.Time,
	operationFunc func(context.Context) error,
) error {
	return retry(ctx, operation, leaseExpiresAt, isBusy, operationFunc)
}

func retry(
	ctx context.Context,
	operation string,
	stopAt time.Time,
	retryable func(error) bool,
	operationFunc func(context.Context) error,
) error {
	const (
		baseDelay = 10 * time.Millisecond
		maxDelay  = 100 * time.Millisecond
	)

	operationCtx := ctx

	if !stopAt.IsZero() {
		var cancel context.CancelFunc

		operationCtx, cancel = context.WithDeadline(ctx, stopAt)
		defer cancel()
	}

	for attempt := 1; attempt <= retryAttempts; attempt++ {
		err := operationFunc(operationCtx)
		if err == nil || !retryable(err) || attempt == retryAttempts {
			return err
		}

		delay, withinDeadline := retryDelay(stopAt, baseDelay, maxDelay, attempt)
		if !withinDeadline {
			return err
		}

		timer := time.NewTimer(delay)
		select {
		case <-operationCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return fmt.Errorf("%s: %w", operation, operationCtx.Err())
		case <-timer.C:
		}
	}

	return nil
}

func retryDelay(stopAt time.Time, baseDelay, maxDelay time.Duration, attempt int) (time.Duration, bool) {
	delay := backoff.FullJitter(baseDelay, maxDelay, attempt)
	if stopAt.IsZero() {
		return delay, true
	}

	remaining := time.Until(stopAt)
	if remaining <= 0 {
		return 0, false
	}

	return min(delay, remaining), true
}
