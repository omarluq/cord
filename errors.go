package cord

import (
	"errors"
	"fmt"
)

var (
	// ErrRunCanceled indicates that a durable workflow run was canceled.
	ErrRunCanceled = errors.New("cord: workflow run was canceled")
	// ErrRunNotFound indicates that no durable run exists with the supplied ID.
	ErrRunNotFound = errors.New("cord: workflow run not found")
	// ErrRunFinished indicates that cancellation lost to workflow completion or failure.
	ErrRunFinished = errors.New("cord: workflow run already finished")
	// ErrRunConflict indicates that an idempotency key belongs to a different submission.
	ErrRunConflict = errors.New("cord: workflow submission conflicts with an existing run")
	// ErrRunIncompatible indicates that a workflow handle is incompatible with a durable run.
	ErrRunIncompatible = errors.New("cord: workflow is incompatible with the durable run")
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
