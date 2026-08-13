package cord

import (
	"context"
	"fmt"
)

type panicError struct {
	value any
}

func (err panicError) Error() string {
	return fmt.Sprintf("cord: workflow step panicked: %v", err.value)
}

func (err panicError) Unwrap() error {
	wrapped, ok := err.value.(error)
	if !ok {
		return nil
	}

	return wrapped
}

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("cord: workflow context: %w", err)
	}

	return nil
}
