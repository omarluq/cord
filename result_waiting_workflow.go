package cord

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
)

const resultPollInterval = 100 * time.Millisecond

func (w Workflow[I, O]) wait(
	ctx context.Context,
	runID storage.RunID,
	codec serialization.JSONCodec[O],
	observeRuntimeClose bool,
) (O, error) {
	return w.waitResult(ctx, runID, codec, observeRuntimeClose, nil)
}

func (w Workflow[I, O]) waitResult(
	ctx context.Context,
	runID storage.RunID,
	codec serialization.JSONCodec[O],
	observeRuntimeClose bool,
	verify func(*storage.RunResult) error,
) (O, error) {
	var zero O

	observations, unsubscribe := w.runtime.subscribeCompletion(runID, observeRuntimeClose)
	defer unsubscribe()

	for {
		observation, err := waitForResultObservation(
			ctx, w.runtime.ctx, observations, observeRuntimeClose,
		)
		if err != nil {
			return zero, err
		}

		if readErr := publicResultReadError(ctx, observation.err); readErr != nil {
			return zero, readErr
		}

		value, done, resultErr := w.verifiedResult(&observation.result, codec, verify)
		if done {
			return value, resultErr
		}
	}
}

func publicResultReadError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	if ctx.Err() != nil {
		return fmt.Errorf("cord: workflow context: %w", ctx.Err())
	}

	if errors.Is(err, storage.ErrRunNotFound) {
		return fmt.Errorf("cord: read run result: %w", ErrRunNotFound)
	}

	return fmt.Errorf("cord: read run result: %w", err)
}

func (w Workflow[I, O]) verifiedResult(
	result *storage.RunResult,
	codec serialization.JSONCodec[O],
	verify func(*storage.RunResult) error,
) (value O, done bool, err error) {
	var zero O

	if verify != nil {
		if err := verify(result); err != nil {
			return zero, true, err
		}
	}

	return w.result(result, codec)
}

func waitForResultObservation(
	ctx context.Context,
	runtimeCtx context.Context,
	observations <-chan completionObservation,
	observeRuntimeClose bool,
) (completionObservation, error) {
	var runtimeDone <-chan struct{}
	if observeRuntimeClose {
		runtimeDone = runtimeCtx.Done()
	}

	select {
	case <-runtimeDone:
		return completionObservation{}, errors.New("cord: runtime closed")
	case <-ctx.Done():
		return completionObservation{}, fmt.Errorf("cord: workflow context: %w", ctx.Err())
	case observation := <-observations:
		return observation, nil
	}
}

func (w Workflow[I, O]) result(
	result *storage.RunResult,
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
		return zero, true, ErrRunCanceled
	case storage.RunRunning, storage.RunCanceling:
		return zero, false, nil
	}

	return zero, true, fmt.Errorf("cord: unknown workflow run status %q", result.Status)
}
