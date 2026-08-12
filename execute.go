package cord

import (
	"context"
	"errors"
	"fmt"
)

type nodeResult struct {
	output any
	err    error
	id     nodeID
}

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

type execution struct {
	ctx       context.Context
	err       error
	input     any
	results   chan nodeResult
	slots     chan struct{}
	children  map[nodeID][]nodeID
	outputs   map[nodeID]any
	remaining map[nodeID]int
	nodes     map[nodeID]node
	cancel    context.CancelFunc
	ready     []nodeID
	active    int
	completed int
}

func execute(ctx context.Context, plan []node, tail nodeID, input any, slots chan struct{}) (any, error) {
	if ctx == nil {
		return nil, errors.New("cord: workflow context is nil")
	}

	if slots == nil {
		return nil, errors.New("cord: workflow runtime is invalid")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	run := newExecution(runCtx, cancel, plan, input, slots)
	for run.completed < len(plan) && run.err == nil {
		run.schedule()
		run.await()
	}

	if run.err != nil {
		cancel()
		run.drain()

		return nil, run.err
	}

	return run.outputs[tail], nil
}

func newExecution(
	ctx context.Context,
	cancel context.CancelFunc,
	plan []node,
	input any,
	slots chan struct{},
) *execution {
	run := &execution{
		ctx:       ctx,
		cancel:    cancel,
		nodes:     make(map[nodeID]node, len(plan)),
		remaining: make(map[nodeID]int, len(plan)),
		children:  make(map[nodeID][]nodeID, len(plan)),
		outputs:   make(map[nodeID]any, len(plan)),
		results:   make(chan nodeResult, len(plan)),
		slots:     slots,
		ready:     make([]nodeID, 0, len(plan)),
		input:     input,
		active:    0,
		completed: 0,
		err:       nil,
	}

	for _, current := range plan {
		run.add(current)
	}

	return run
}

func (run *execution) add(current node) {
	run.nodes[current.id] = current
	run.remaining[current.id] = len(current.parents)

	if len(current.parents) == 0 {
		run.ready = append(run.ready, current.id)
	}

	for _, parent := range current.parents {
		run.children[parent] = append(run.children[parent], current.id)
	}
}

func (run *execution) schedule() {
	if run.err == nil {
		run.err = contextError(run.ctx)
	}

	for run.err == nil && len(run.ready) > 0 {
		if !run.acquireSlot() {
			return
		}

		nodeIdentifier := run.ready[0]
		run.ready = run.ready[1:]
		current := run.nodes[nodeIdentifier]
		inputs := nodeInputs(current, run.input, run.outputs)
		run.active++

		go run.invoke(current, inputs)
	}
}

func (run *execution) acquireSlot() bool {
	select {
	case run.slots <- struct{}{}:
		return true
	default:
	}

	if run.active > 0 {
		return false
	}

	select {
	case run.slots <- struct{}{}:
		return true
	case <-run.ctx.Done():
		run.err = contextError(run.ctx)

		return false
	}
}

func (run *execution) invoke(current node, inputs []any) {
	result := nodeResult{
		output: nil,
		err:    nil,
		id:     current.id,
	}

	defer func() {
		<-run.slots

		if recovered := recover(); recovered != nil {
			result.err = panicError{value: recovered}
		}

		run.results <- result
	}()

	result.output, result.err = current.invoke(run.ctx, inputs)
}

func (run *execution) drain() {
	for run.active > 0 {
		<-run.results
		run.active--
	}
}

func (run *execution) await() {
	if run.active == 0 {
		if run.err == nil {
			run.err = errors.New("cord: workflow graph cannot make progress")
		}

		return
	}

	result := <-run.results
	run.active--
	run.completed++

	if run.err == nil {
		run.err = contextError(run.ctx)
	}

	if result.err != nil && run.err == nil {
		run.err = result.err
		run.cancel()
	}

	if run.err == nil {
		run.release(result)
	}
}

func (run *execution) release(result nodeResult) {
	run.outputs[result.id] = result.output
	for _, child := range run.children[result.id] {
		run.remaining[child]--
		if run.remaining[child] == 0 {
			run.ready = append(run.ready, child)
		}
	}
}

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("cord: workflow context: %w", err)
	}

	return nil
}

func nodeInputs(current node, rootInput any, outputs map[nodeID]any) []any {
	if len(current.parents) == 0 {
		return []any{rootInput}
	}

	inputs := make([]any, 0, len(current.parents))
	for _, parent := range current.parents {
		inputs = append(inputs, outputs[parent])
	}

	return inputs
}
