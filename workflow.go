package cord

import (
	"context"
	"errors"
)

// Workflow is an immutable typed handle to a terminal node in a workflow graph.
type Workflow[I, O any] struct {
	runtime *Cord
	graph   *graph
	err     error
	tail    nodeID
}

// Then appends fn after the workflow's current terminal node and returns a new handle.
func (w Workflow[I, O]) Then[N any](
	step func(context.Context, O) (N, error),
) Workflow[I, N] {
	if w.err != nil {
		return Workflow[I, N](w)
	}

	if w.runtime == nil || w.graph == nil {
		return Workflow[I, N]{
			runtime: nil,
			graph:   nil,
			tail:    0,
			err:     errors.New("cord: invalid workflow"),
		}
	}

	if step == nil {
		return Workflow[I, N]{
			runtime: w.runtime,
			graph:   w.graph,
			tail:    w.tail,
			err:     errors.New("cord: workflow step is nil"),
		}
	}

	definition := stepDefinition(step)
	registrationErr := w.runtime.register(definition, encodedStep(step))
	tail := w.graph.appendNode([]nodeID{w.tail}, definition)

	return Workflow[I, N]{
		runtime: w.runtime,
		graph:   w.graph,
		tail:    tail,
		err:     errors.Join(w.err, registrationErr),
	}
}
