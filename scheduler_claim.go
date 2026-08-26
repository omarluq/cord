package cord

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const terminalCommitTimeout = 5 * time.Second

func (c *Cord) executeClaim(claim *storage.Claim) {
	defer c.goroutineDone()
	defer func() { <-c.slots; c.signalScheduler() }()

	c.mu.RLock()
	invocation, ok := c.registry[claim.FunctionKey]
	c.mu.RUnlock()

	if !ok || invocation.signature != claim.SignatureHash {
		c.releaseClaim(claim, errors.New("cord: claimed node registration is unavailable"))

		return
	}

	executionCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	heartbeatDone := make(chan bool, 1)
	go c.heartbeat(executionCtx, claim, cancel, heartbeatDone)

	output, leaseHeld, invokeErr := c.invokeClaim(
		executionCtx,
		claim,
		invocation,
		cancel,
		heartbeatDone,
	)
	if !leaseHeld || c.ctx.Err() != nil {
		return
	}

	commitCtx, commitCancel := context.WithTimeout(context.Background(), terminalCommitTimeout)
	defer commitCancel()

	var transitionErr error
	if invokeErr == nil {
		transitionErr = c.completeClaim(commitCtx, claim, output)
	} else {
		transitionErr = c.handleFailure(commitCtx, claim, invokeErr)
	}

	if transitionErr != nil {
		c.reportClaimTransitionError(transitionErr)
	}
}

func (c *Cord) invokeClaim(
	executionCtx context.Context,
	claim *storage.Claim,
	invocation registeredInvocation,
	cancel context.CancelFunc,
	heartbeatDone <-chan bool,
) (storage.EncodedPayload, bool, error) {
	unregister, runnable, err := c.registerActiveAttempt(executionCtx, claim, cancel)
	if err != nil {
		leaseHeld := <-heartbeatDone
		if leaseHeld && c.ctx.Err() == nil && !errors.Is(err, context.Canceled) {
			c.releaseClaim(claim, err)
		}

		return nil, false, nil
	}

	if !runnable {
		<-heartbeatDone

		return nil, false, nil
	}

	defer unregister()

	inputs, err := c.store.LoadNodeInputs(executionCtx, claim.RunID, claim.NodeID)
	if err != nil {
		cancel()

		leaseHeld := <-heartbeatDone
		if leaseHeld && c.ctx.Err() == nil {
			c.releaseClaim(claim, fmt.Errorf("cord: load claimed node inputs: %w", err))
		}

		return nil, false, nil
	}

	output, invokeErr := invokeSafely(executionCtx, invocation.invoke, inputs)

	cancel()

	return output, <-heartbeatDone, invokeErr
}

func (c *Cord) registerActiveAttempt(
	executionCtx context.Context,
	claim *storage.Claim,
	cancel context.CancelFunc,
) (unregister func(), runnable bool, registrationErr error) {
	// Register before the durable check so a local cancellation racing this
	// query either finds the attempt or is observed by the query afterward.
	c.activeMu.Lock()
	unregister = c.registerActiveAttemptLocked(claim, cancel)
	c.activeMu.Unlock()

	result, err := c.store.GetRunResult(executionCtx, claim.RunID)
	if err != nil {
		executionErr := executionCtx.Err()

		unregister()
		cancel()

		if executionErr != nil {
			return noopUnregister, false, fmt.Errorf("cord: verify claimed run status: %w", executionErr)
		}

		return noopUnregister, false, fmt.Errorf("cord: verify claimed run status: %w", err)
	}

	var statusErr error

	switch result.Status {
	case storage.RunRunning:
		return unregister, true, nil
	case storage.RunCompleted, storage.RunFailed, storage.RunCanceling, storage.RunCanceled:
	case storage.RunStatus(""):
		statusErr = errors.New("cord: verify claimed run status: durable run status is empty")
	default:
		statusErr = fmt.Errorf("cord: verify claimed run status: invalid durable run status %q", result.Status)
	}

	unregister()
	cancel()

	return noopUnregister, false, statusErr
}

func noopUnregister() {
	// The active attempt was already unregistered before this callback was returned.
}

func invokeSafely(
	ctx context.Context,
	invoke encodedInvocation,
	inputs []storage.EncodedPayload,
) (output storage.EncodedPayload, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError{value: recovered}
		}
	}()

	return invoke(ctx, inputs)
}
