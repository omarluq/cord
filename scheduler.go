package cord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
)

const (
	defaultLeaseTTL          = 30 * time.Second
	defaultHeartbeatInterval = 10 * time.Second
	defaultPollInterval      = 200 * time.Millisecond
	terminalCommitTimeout    = 5 * time.Second
	joinedInputCount         = 2
)

type encodedInvocation func(context.Context, []storage.EncodedPayload) (storage.EncodedPayload, error)

type registeredInvocation struct {
	invoke    encodedInvocation
	signature string
}

type persistedFailure struct {
	Time        time.Time `json:"time"`
	Message     string    `json:"message"`
	NodeID      string    `json:"node_id"`
	FunctionKey string    `json:"function_key"`
	Attempt     int       `json:"attempt"`
	Retryable   bool      `json:"retryable"`
}

type runFailureError struct{ failure *persistedFailure }

func (err runFailureError) Error() string { return err.failure.Message }

func encodedStep[I, O any](step func(context.Context, I) (O, error)) encodedInvocation {
	inputCodec, inputErr := serialization.NewJSONCodec[I]()
	outputCodec, outputErr := serialization.NewJSONCodec[O]()
	codecErr := errors.Join(inputErr, outputErr)

	return func(ctx context.Context, inputs []storage.EncodedPayload) (storage.EncodedPayload, error) {
		if codecErr != nil {
			return nil, codecErr
		}

		if len(inputs) != 1 {
			return nil, errors.New("cord: invalid persistent workflow node input")
		}

		input, err := inputCodec.Decode(inputs[0])
		if err != nil {
			return nil, fmt.Errorf("cord: decode persistent workflow node input: %w", err)
		}

		output, err := step(ctx, input)
		if err != nil {
			return nil, err
		}

		payload, err := outputCodec.Encode(output)
		if err != nil {
			return nil, fmt.Errorf("cord: encode persistent workflow node output: %w", err)
		}

		return storage.EncodedPayload(payload), nil
	}
}

func encodedJoin[A, B, O any](step func(context.Context, A, B) (O, error)) encodedInvocation {
	leftCodec, leftErr := serialization.NewJSONCodec[A]()
	rightCodec, rightErr := serialization.NewJSONCodec[B]()
	outputCodec, outputErr := serialization.NewJSONCodec[O]()
	codecErr := errors.Join(leftErr, rightErr, outputErr)

	return func(ctx context.Context, inputs []storage.EncodedPayload) (storage.EncodedPayload, error) {
		if codecErr != nil {
			return nil, codecErr
		}

		if len(inputs) != joinedInputCount {
			return nil, errors.New("cord: invalid persistent joined workflow node input")
		}

		left, leftErr := leftCodec.Decode(inputs[0])

		right, rightErr := rightCodec.Decode(inputs[1])
		if err := errors.Join(leftErr, rightErr); err != nil {
			return nil, fmt.Errorf("cord: decode persistent joined workflow node input: %w", err)
		}

		output, err := step(ctx, left, right)
		if err != nil {
			return nil, err
		}

		payload, err := outputCodec.Encode(output)
		if err != nil {
			return nil, fmt.Errorf("cord: encode persistent joined workflow node output: %w", err)
		}

		return storage.EncodedPayload(payload), nil
	}
}

func (c *Cord) register(definition nodeDefinition, invoke encodedInvocation) error {
	if definition.err != nil {
		return definition.err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.registry[definition.functionKey]; ok {
		if existing.signature != definition.signature {
			return fmt.Errorf("cord: conflicting registration for %q", definition.functionKey)
		}

		return nil
	}

	c.registry[definition.functionKey] = registeredInvocation{invoke: invoke, signature: definition.signature}
	c.registryJSON = nil

	return nil
}

func (c *Cord) signalScheduler() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Cord) scheduler() {
	defer c.workers.Done()

	pollTimer := time.NewTimer(c.pollInterval)
	defer pollTimer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
			c.drainReadyNodes()
		case <-pollTimer.C:
			c.poll()
			pollTimer.Reset(c.pollInterval)
		}
	}
}

func (c *Cord) poll() {
	if err := c.maintain(); err != nil {
		c.reportSchedulerError(fmt.Errorf("cord: scheduler maintenance: %w", err))

		return
	}

	c.drainReadyNodes()
}

func (c *Cord) drainReadyNodes() {
	for c.trySchedule() {
	}
}

func (c *Cord) maintain() error {
	if _, err := c.store.PromoteRetries(c.ctx); err != nil {
		return fmt.Errorf("cord: promote retries: %w", err)
	}

	if _, err := c.store.RecoverExpiredLeases(c.ctx); err != nil {
		return fmt.Errorf("cord: recover expired leases: %w", err)
	}

	return nil
}

func (c *Cord) trySchedule() bool {
	select {
	case c.slots <- struct{}{}:
	default:
		return false
	}

	registeredFunctions, err := c.registeredFunctions()
	if err != nil {
		<-c.slots
		c.reportSchedulerError(err)

		return false
	}

	claim, ok, err := c.store.ClaimReadyNodeForFunctions(c.ctx, c.owner, c.leaseTTL, registeredFunctions)
	if err != nil || !ok {
		<-c.slots

		if err != nil && c.ctx.Err() == nil {
			c.reportSchedulerError(fmt.Errorf("cord: scheduler claim: %w", err))
		}

		return false
	}

	c.workers.Add(1)
	go c.executeClaim(claim)

	return true
}

func (c *Cord) registeredFunctions() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.registry) == 0 || c.registryJSON != nil {
		return c.registryJSON, nil
	}

	registrations := make(map[string]string, len(c.registry))
	for key, entry := range c.registry {
		registrations[key] = entry.signature
	}

	registryJSON, err := json.Marshal(registrations)
	if err != nil {
		return nil, fmt.Errorf("cord: encode function registry: %w", err)
	}

	c.registryJSON = registryJSON

	return c.registryJSON, nil
}

func (c *Cord) executeClaim(claim *storage.Claim) {
	defer c.workers.Done()
	defer func() { <-c.slots; c.signalScheduler() }()

	c.mu.RLock()
	registered, ok := c.registry[claim.FunctionKey]
	c.mu.RUnlock()

	if !ok || registered.signature != claim.SignatureHash {
		c.releaseClaim(claim, errors.New("cord: claimed node registration is unavailable"))

		return
	}

	inputs, err := c.store.LoadNodeInputs(c.ctx, claim.RunID, claim.NodeID)
	if err != nil {
		c.releaseClaim(claim, fmt.Errorf("cord: load claimed node inputs: %w", err))

		return
	}

	executionCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	heartbeatDone := make(chan bool, 1)
	go c.heartbeat(executionCtx, claim, cancel, heartbeatDone)

	output, invokeErr := invokeSafely(executionCtx, registered.invoke, inputs)

	cancel()

	leaseHeld := <-heartbeatDone
	if !leaseHeld || c.ctx.Err() != nil {
		return
	}

	commitCtx, commitCancel := context.WithTimeout(context.Background(), terminalCommitTimeout)
	defer commitCancel()

	if invokeErr == nil {
		_, err = c.store.CompleteNode(commitCtx, claim.RunID, claim.NodeID, claim.Lease, output)
	} else {
		err = c.handleFailure(commitCtx, claim, invokeErr)
	}

	if err != nil {
		c.reportSchedulerError(err)
	}
}

func (c *Cord) handleFailure(ctx context.Context, claim *storage.Claim, invokeErr error) error {
	failure := encodeFailure(claim, invokeErr)

	policy := retryPolicy{
		maxAttempts: claim.MaxAttempts,
		baseDelay:   claim.RetryBaseDelay,
		maxDelay:    claim.RetryMaxDelay,
	}

	if isPermanent(invokeErr) || claim.Attempt >= policy.maxAttempts {
		_, err := c.store.FailNode(ctx, claim.RunID, claim.NodeID, claim.Lease, failure)
		if err != nil {
			return fmt.Errorf("cord: fail node: %w", err)
		}

		return nil
	}

	delay := retryDelay(policy, claim.Attempt)

	_, err := c.store.RetryNode(ctx, claim.RunID, claim.NodeID, claim.Lease, failure, delay)
	if err != nil {
		return fmt.Errorf("cord: schedule retry: %w", err)
	}

	return nil
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

func encodeFailure(claim *storage.Claim, err error) storage.EncodedPayload {
	failure := persistedFailure{
		Message: err.Error(), NodeID: string(claim.NodeID), FunctionKey: claim.FunctionKey,
		Time: time.Now().UTC(), Attempt: claim.Attempt, Retryable: !isPermanent(err),
	}

	payload, marshalErr := json.Marshal(failure)
	if marshalErr != nil {
		return storage.EncodedPayload([]byte(`{"message":"cord: encode failure"}`))
	}

	return storage.EncodedPayload(payload)
}

func (c *Cord) heartbeat(ctx context.Context, claim *storage.Claim, cancel context.CancelFunc, done chan<- bool) {
	held := true
	defer func() { done <- held }()

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	expires := claim.Lease.ExpiresAt

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			accepted, newExpiry, err := c.store.HeartbeatNode(ctx, claim.RunID, claim.NodeID, claim.Lease, c.leaseTTL)
			if err == nil && accepted {
				expires = newExpiry

				continue
			}

			if (err == nil && !accepted) || !time.Now().Before(expires) {
				held = false

				cancel()

				return
			}
		}
	}
}

func (c *Cord) releaseClaim(claim *storage.Claim, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), terminalCommitTimeout)
	defer cancel()

	_, err := c.store.RetryNode(ctx, claim.RunID, claim.NodeID, claim.Lease, nil, 0)
	if err != nil {
		cause = errors.Join(cause, fmt.Errorf("cord: release unusable claim: %w", err))
	}

	c.reportSchedulerError(cause)
}

func decodeRunError(payload storage.EncodedPayload) error {
	var failure persistedFailure
	if err := json.Unmarshal(payload, &failure); err != nil || failure.Message == "" {
		return errors.New("cord: workflow failed")
	}

	return runFailureError{failure: &failure}
}
