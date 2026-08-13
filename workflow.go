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
	tail := w.graph.appendNode([]nodeID{w.tail}, adaptStep(step), definition)

	return Workflow[I, N]{
		runtime: w.runtime,
		graph:   w.graph,
		tail:    tail,
		err:     registrationErr,
	}
}

// Run submits the workflow and waits for its terminal result. The context
// controls submission, waiting, and cancellation; it is not persisted as workflow data.
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

	runPlan, err := buildPlan(w.graph.name, plan, w.tail, input)
	if err != nil {
		return zero, err
	}

	if err = w.runtime.store.CreateRun(ctx, runPlan); err != nil {
		return zero, fmt.Errorf("cord: persist run: %w", err)
	}

	w.runtime.signalScheduler()

	resultCodec, err := serialization.NewJSONCodec[O]()
	if err != nil {
		return zero, fmt.Errorf("cord: construct result codec: %w", err)
	}

	return w.wait(ctx, runPlan.Run.ID, resultCodec)
}

func (w Workflow[I, O]) wait(
	ctx context.Context,
	runID storage.RunID,
	codec serialization.JSONCodec[O],
) (O, error) {
	var zero O

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		result, err := w.runtime.store.GetRunResult(ctx, runID)
		if err == nil {
			value, done, resultErr := w.result(ctx, runID, result, codec)
			if done {
				return value, resultErr
			}
		}

		select {
		case <-ctx.Done():
			_, cancelErr := w.runtime.store.CancelRun(context.Background(), runID)
			if cancelErr != nil {
				return zero, errors.Join(contextError(ctx), cancelErr)
			}

			return zero, contextError(ctx)
		case <-ticker.C:
		}
	}
}

func (w Workflow[I, O]) result(
	ctx context.Context,
	runID storage.RunID,
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
		return zero, true, w.runtime.runError(runID, result.Error)
	case storage.RunCanceled:
		return zero, true, contextError(ctx)
	case storage.RunRunning, storage.RunCanceling:
		return zero, false, nil
	}

	return zero, false, nil
}
