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

	tail := w.graph.appendNode([]nodeID{w.tail}, adaptStep(step))

	return Workflow[I, N]{
		runtime: w.runtime,
		graph:   w.graph,
		tail:    tail,
		err:     nil,
	}
}

// Run executes the reachable workflow graph and waits for its terminal result.
func (w Workflow[I, O]) Run(ctx context.Context, input I) (O, error) {
	var zero O

	if w.err != nil {
		return zero, w.err
	}

	if w.runtime == nil || w.graph == nil {
		return zero, errors.New("cord: invalid workflow")
	}

	plan, err := w.graph.compile(w.tail)
	if err != nil {
		return zero, err
	}

	output, err := execute(ctx, plan, w.tail, input, w.runtime.slots)
	if err != nil {
		return zero, err
	}

	result, ok := inputAs[O](output)
	if !ok {
		return zero, errors.New("cord: invalid terminal workflow output")
	}

	return result, nil
}
