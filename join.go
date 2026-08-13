package cord

import (
	"context"
	"errors"
)

// Join combines two branches from the same workflow definition.
func Join[I, A, B any](left Workflow[I, A], right Workflow[I, B]) JoinResult[I, A, B] {
	joined := JoinResult[I, A, B]{
		runtime: left.runtime,
		graph:   left.graph,
		left:    left.tail,
		right:   right.tail,
		err:     errors.Join(left.err, right.err),
	}

	if left.runtime != right.runtime || left.graph != right.graph {
		joined.err = errors.Join(joined.err, errors.New("cord: cannot join unrelated workflows"))
	}

	return joined
}

// JoinResult is a typed handle to two branches awaiting a joined step.
type JoinResult[I, A, B any] struct {
	runtime *Cord
	graph   *graph
	err     error
	left    nodeID
	right   nodeID
}

// Then appends fn after both joined branches and returns a workflow handle.
func (j JoinResult[I, A, B]) Then[O any](
	step func(context.Context, A, B) (O, error),
) Workflow[I, O] {
	if j.err != nil {
		return Workflow[I, O]{
			runtime: j.runtime,
			graph:   j.graph,
			tail:    j.left,
			err:     j.err,
		}
	}

	if j.runtime == nil || j.graph == nil {
		return Workflow[I, O]{
			runtime: nil,
			graph:   nil,
			tail:    0,
			err:     errors.New("cord: invalid join"),
		}
	}

	if step == nil {
		return Workflow[I, O]{
			runtime: j.runtime,
			graph:   j.graph,
			tail:    j.left,
			err:     errors.New("cord: joined workflow step is nil"),
		}
	}

	definition := joinDefinition(step)
	registrationErr := j.runtime.register(definition, encodedJoin(step))
	tail := j.graph.appendNode(
		[]nodeID{j.left, j.right},
		adaptJoin(step),
		definition,
	)

	return Workflow[I, O]{
		runtime: j.runtime,
		graph:   j.graph,
		tail:    tail,
		err:     registrationErr,
	}
}
