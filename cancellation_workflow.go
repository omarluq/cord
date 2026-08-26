package cord

import (
	"context"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

// Cancel durably cancels runID without checking this handle's workflow identity.
// Callers must authorize runID before calling Cancel. Cancellation is idempotent
// for an already canceled run; missing and finished runs return errors matching
// ErrRunNotFound and ErrRunFinished. It cannot forcibly stop non-cooperative user
// code already executing. Active attempts in this runtime are signaled promptly;
// attempts in other runtimes observe cancellation through lease heartbeat failure.
// If a storage response is ambiguous, Cancel reconciles the authoritative durable
// status before returning a definitive result.
func (w Workflow[I, O]) Cancel(ctx context.Context, runID RunID) error {
	if ctx == nil {
		return errors.New("cord: workflow context is nil")
	}

	if runID == "" {
		return errors.New("cord: run ID is empty")
	}

	if w.runtime == nil || w.runtime.store == nil {
		return errors.New("cord: invalid workflow")
	}

	storageRunID := storage.RunID(runID)

	outcome, err := w.runtime.store.CancelRun(ctx, storageRunID)
	if err != nil {
		return w.reconcileCancellation(ctx, storageRunID, err)
	}

	return w.cancellationResult(storageRunID, outcome)
}

func (w Workflow[I, O]) reconcileCancellation(
	ctx context.Context,
	runID storage.RunID,
	operationErr error,
) error {
	return w.reconcileCancellationOnce(ctx, runID, operationErr, true)
}

func (w Workflow[I, O]) reconcileCancellationOnce(
	ctx context.Context,
	runID storage.RunID,
	operationErr error,
	mayRetry bool,
) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return cancellationError(operationErr, ctxErr)
	}

	result, err := w.runtime.store.GetRunResult(ctx, runID)
	if err != nil {
		return cancellationReconciliationError(operationErr, err)
	}

	if result.Status == storage.RunCanceling {
		if !mayRetry {
			return cancellationError(operationErr, errors.New("durable cancellation remains pending"))
		}

		outcome, retryErr := w.runtime.store.CancelRun(ctx, runID)
		if retryErr == nil {
			resultErr := w.cancellationResult(runID, outcome)
			if resultErr != nil && !errors.Is(resultErr, ErrRunFinished) &&
				!errors.Is(resultErr, ErrRunNotFound) {
				return cancellationError(operationErr, resultErr)
			}

			return resultErr
		}

		return w.reconcileCancellationOnce(ctx, runID, errors.Join(operationErr, retryErr), false)
	}

	return w.reconciledCancellationResult(runID, result.Status, operationErr)
}

func cancellationReconciliationError(operationErr, reconciliationErr error) error {
	switch {
	case errors.Is(reconciliationErr, storage.ErrRunNotFound):
		return ErrRunNotFound
	case errors.Is(reconciliationErr, storage.ErrRunIncompatible):
		return cancellationError(operationErr, ErrRunIncompatible, reconciliationErr)
	default:
		return cancellationError(operationErr, reconciliationErr)
	}
}

func (w Workflow[I, O]) reconciledCancellationResult(
	runID storage.RunID,
	status storage.RunStatus,
	operationErr error,
) error {
	switch status {
	case storage.RunCanceled:
		w.runtime.notifyCompletion(runID)

		return nil
	case storage.RunCompleted, storage.RunFailed:
		return ErrRunFinished
	case storage.RunRunning:
		return cancellationError(operationErr)
	case storage.RunCanceling:
		return cancellationError(operationErr, errors.New("durable cancellation remains pending"))
	default:
		return cancellationError(operationErr, fmt.Errorf("unknown durable run status %q", status))
	}
}

func cancellationError(errs ...error) error {
	return fmt.Errorf("cord: cancel run: %w", errors.Join(errs...))
}

func (w Workflow[I, O]) cancellationResult(
	runID storage.RunID,
	outcome storage.CancellationOutcome,
) error {
	switch outcome {
	case storage.CancellationCanceled, storage.CancellationAlreadyCanceled:
		w.runtime.notifyCompletion(runID)

		return nil
	case storage.CancellationFinished:
		return ErrRunFinished
	case storage.CancellationNotFound:
		return ErrRunNotFound
	default:
		return fmt.Errorf("cord: unknown run cancellation outcome %q", outcome)
	}
}
