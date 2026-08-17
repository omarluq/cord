package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/backoff"
)

const retryAttempts = 20

func retryContention(ctx context.Context, operation string, operationFunc func() error) error {
	return retry(ctx, operation, isBusy, operationFunc)
}

func retry(
	ctx context.Context,
	operation string,
	retryable func(error) bool,
	operationFunc func() error,
) error {
	const (
		baseDelay = 10 * time.Millisecond
		maxDelay  = 100 * time.Millisecond
	)

	for attempt := 1; attempt <= retryAttempts; attempt++ {
		err := operationFunc()
		if err == nil || !retryable(err) || attempt == retryAttempts {
			return err
		}

		delay := backoff.FullJitter(baseDelay, maxDelay, attempt)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return fmt.Errorf("%s: %w", operation, ctx.Err())
		case <-timer.C:
		}
	}

	return nil
}
