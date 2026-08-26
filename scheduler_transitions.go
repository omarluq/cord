package cord

import (
	"context"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

type fencedTransitionClass uint8

const (
	fencedTransitionLeaseLost fencedTransitionClass = iota
	fencedTransitionDurableWinner
	fencedTransitionCancellationWon
	fencedTransitionImpossibleState
)

type fencedTransitionError struct {
	operation string
	status    storage.RunStatus
	class     fencedTransitionClass
}

// Error describes why durable storage rejected a scheduler transition.
func (err *fencedTransitionError) Error() string {
	switch err.class {
	case fencedTransitionLeaseLost:
		return fmt.Sprintf("cord: %s rejected: lease ownership was lost", err.operation)
	case fencedTransitionDurableWinner:
		return fmt.Sprintf("cord: %s rejected: durable run outcome %q already won", err.operation, err.status)
	case fencedTransitionCancellationWon:
		return fmt.Sprintf("cord: %s rejected: run cancellation already won", err.operation)
	case fencedTransitionImpossibleState:
		return fmt.Sprintf("cord: %s rejected: impossible durable run status %q", err.operation, err.status)
	}

	return fmt.Sprintf("cord: %s rejected: unknown transition classification", err.operation)
}

func (c *Cord) reportClaimTransitionError(err error) {
	if isExpectedRejectedTransition(err) {
		return
	}

	c.reportSchedulerError(err)
}

func isExpectedRejectedTransition(err error) bool {
	rejected := &fencedTransitionError{}

	if !errors.As(err, &rejected) {
		return false
	}

	return rejected.class == fencedTransitionCancellationWon ||
		rejected.class == fencedTransitionDurableWinner
}

func (c *Cord) completeClaim(
	ctx context.Context,
	claim *storage.Claim,
	output storage.EncodedPayload,
) error {
	accepted, err := c.store.CompleteNode(ctx, claim.RunID, claim.NodeID, claim.Lease, output)
	if err != nil {
		return fmt.Errorf("cord: complete node: %w", err)
	}

	if !accepted {
		return c.classifyRejectedTransition(ctx, claim.RunID, "node completion")
	}

	c.notifyWaiters(claim.RunID)

	return nil
}

func (c *Cord) handleFailure(ctx context.Context, claim *storage.Claim, invokeErr error) error {
	failure := encodeFailure(claim, invokeErr)

	policy := retryPolicy{
		maxAttempts: claim.MaxAttempts,
		baseDelay:   claim.RetryBaseDelay,
		maxDelay:    claim.RetryMaxDelay,
	}

	permanent := isPermanent(invokeErr)
	if permanent || claim.Attempt >= policy.maxAttempts {
		reason := storage.ReasonFailureAttemptsExhausted
		if permanent {
			reason = storage.ReasonFailureNonRetryable
		}

		accepted, err := c.store.FailNode(ctx, claim.RunID, claim.NodeID, claim.Lease, failure, reason)
		if err != nil {
			return fmt.Errorf("cord: fail node: %w", err)
		}

		if !accepted {
			return c.classifyRejectedTransition(ctx, claim.RunID, "node failure")
		}

		c.notifyWaiters(claim.RunID)

		return nil
	}

	delay := retryDelay(policy, claim.Attempt)

	accepted, err := c.store.RetryNode(ctx, claim.RunID, claim.NodeID, claim.Lease, failure, delay)
	if err != nil {
		return fmt.Errorf("cord: schedule retry: %w", err)
	}

	if !accepted {
		return c.classifyRejectedTransition(ctx, claim.RunID, "node retry")
	}

	return nil
}

func (c *Cord) classifyRejectedTransition(
	ctx context.Context,
	runID storage.RunID,
	operation string,
) error {
	result, err := c.store.GetRunResult(ctx, runID)
	if err != nil {
		if errors.Is(err, storage.ErrRunNotFound) {
			return &fencedTransitionError{
				operation: operation,
				class:     fencedTransitionImpossibleState,
			}
		}

		return fmt.Errorf("cord: classify rejected %s: %w", operation, err)
	}

	class := fencedTransitionImpossibleState

	switch result.Status {
	case storage.RunRunning:
		class = fencedTransitionLeaseLost
	case storage.RunCompleted, storage.RunFailed:
		class = fencedTransitionDurableWinner
	case storage.RunCanceling, storage.RunCanceled:
		class = fencedTransitionCancellationWon
	}

	return &fencedTransitionError{operation: operation, status: result.Status, class: class}
}

func (c *Cord) releaseClaim(claim *storage.Claim, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), terminalCommitTimeout)
	defer cancel()

	accepted, err := c.store.RetryNode(ctx, claim.RunID, claim.NodeID, claim.Lease, nil, 0)
	if err != nil {
		cause = errors.Join(cause, fmt.Errorf("cord: release unusable claim: %w", err))
	} else if !accepted {
		rejectionErr := c.classifyRejectedTransition(ctx, claim.RunID, "claim release")
		if isExpectedRejectedTransition(rejectionErr) {
			return
		}

		cause = errors.Join(cause, rejectionErr)
	}

	c.reportSchedulerError(cause)
}
