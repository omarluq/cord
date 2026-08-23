package cord

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
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

// Run submits the workflow and waits for its terminal result. The context
// controls submission and waiting; canceling it does not cancel the durable run.
func (w Workflow[I, O]) Run(ctx context.Context, input I) (O, error) {
	var zero O

	if ctx == nil {
		return zero, errors.New("cord: workflow context is nil")
	}

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

	runPlan, err := buildPlan(w.graph.name, plan, w.tail, input, w.runtime.retry)
	if err != nil {
		return zero, err
	}

	resultCodec, err := serialization.NewJSONCodec[O]()
	if err != nil {
		return zero, fmt.Errorf("cord: construct result codec: %w", err)
	}

	if !w.runtime.admitRun() {
		return zero, errors.New("cord: runtime closed")
	}

	persistErr := func() error {
		defer w.runtime.finishRunAdmission()

		return w.runtime.store.CreateRun(ctx, runPlan)
	}()
	if persistErr != nil {
		return zero, fmt.Errorf("cord: persist run: %w", persistErr)
	}

	w.runtime.signalScheduler()

	// Shutdown waits for this admitted persistence attempt before canceling the
	// runtime, so a durable run cannot be hidden behind runtime closed here.
	return w.wait(ctx, runPlan.Run.ID, resultCodec, true)
}

const resultPollInterval = 100 * time.Millisecond

func (w Workflow[I, O]) wait(
	ctx context.Context,
	runID storage.RunID,
	codec serialization.JSONCodec[O],
	observeRuntimeClose bool,
) (O, error) {
	var zero O

	completed, unsubscribe := w.runtime.subscribeCompletion(runID)
	defer unsubscribe()

	ticker := time.NewTicker(resultPollInterval)
	defer ticker.Stop()

	for {
		result, err := w.runtime.store.GetRunResult(ctx, runID)
		if err != nil && ctx.Err() == nil {
			return zero, fmt.Errorf("cord: read run result: %w", err)
		}

		if err == nil {
			value, done, resultErr := w.result(result, codec)
			if done {
				return value, resultErr
			}
		}

		if err := waitForResultSignal(
			ctx, w.runtime.ctx, completed, ticker.C, observeRuntimeClose,
		); err != nil {
			return zero, err
		}
	}
}

func waitForResultSignal(
	ctx context.Context,
	runtimeCtx context.Context,
	completed <-chan struct{},
	poll <-chan time.Time,
	observeRuntimeClose bool,
) error {
	var runtimeDone <-chan struct{}
	if observeRuntimeClose {
		runtimeDone = runtimeCtx.Done()
	}

	select {
	case <-runtimeDone:
		return errors.New("cord: runtime closed")
	case <-ctx.Done():
		return fmt.Errorf("cord: workflow context: %w", ctx.Err())
	case <-completed:
		return nil
	case <-poll:
		return nil
	}
}

func (w Workflow[I, O]) result(
	result storage.RunResult,
	codec serialization.JSONCodec[O],
) (value O, done bool, err error) {
	var zero O

	switch result.Status {
	case storage.RunCompleted:
		value, decodeErr := codec.Decode(result.Output)
		if decodeErr != nil {
			return zero, true, fmt.Errorf("cord: decode terminal workflow output: %w", decodeErr)
		}

		return value, true, nil
	case storage.RunFailed:
		return zero, true, decodeRunError(result.Error)
	case storage.RunCanceled:
		return zero, true, errRunCanceled
	case storage.RunRunning, storage.RunCanceling:
		return zero, false, nil
	}

	return zero, true, fmt.Errorf("cord: unknown workflow run status %q", result.Status)
}
