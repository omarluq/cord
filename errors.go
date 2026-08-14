package cord

import (
	"errors"
	"fmt"
)

var errRunCanceled = errors.New("cord: workflow run was canceled")

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
