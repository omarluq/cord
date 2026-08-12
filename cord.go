// Package cord composes typed Go functions into executable workflow graphs.
package cord

import (
	"context"
	"errors"
	"runtime"
)

// Cord is a process-local workflow runtime. Its concurrency limit is shared by
// all workflows and concurrent runs created from it.
type Cord struct {
	slots chan struct{}
}

// New creates a process-local workflow runtime.
func New() *Cord {
	return newCord(max(1, runtime.GOMAXPROCS(0)))
}

func newCord(concurrency int) *Cord {
	return &Cord{
		slots: make(chan struct{}, max(1, concurrency)),
	}
}

// From creates a workflow whose root node invokes step.
func (c *Cord) From[I, O any](
	name string,
	step func(context.Context, I) (O, error),
) Workflow[I, O] {
	graph := newGraph(name)
	if step == nil {
		return Workflow[I, O]{
			runtime: c,
			graph:   graph,
			tail:    0,
			err:     errors.New("cord: root step is nil"),
		}
	}

	tail := graph.appendNode([]nodeID{}, adaptStep(step))

	return Workflow[I, O]{
		runtime: c,
		graph:   graph,
		tail:    tail,
		err:     nil,
	}
}
