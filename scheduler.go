package cord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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
	c.registrations = nil

	return nil
}

func (c *Cord) signalScheduler() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Cord) scheduler() {
	defer c.goroutineDone()

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

	registeredFunctions := c.registeredFunctions()

	claim, ok, err := c.store.ClaimReadyNodeForFunctions(c.ctx, c.owner, c.leaseTTL, registeredFunctions)
	if err != nil || !ok {
		<-c.slots

		if err != nil && c.ctx.Err() == nil {
			c.reportSchedulerError(fmt.Errorf("cord: scheduler claim: %w", err))
		}

		return false
	}

	c.addGoroutine()
	go c.executeClaim(claim)

	return true
}

func (c *Cord) registeredFunctions() []storage.FunctionRegistration {
	c.mu.RLock()

	if len(c.registry) == 0 || c.registrations != nil {
		registrations := c.registrations
		c.mu.RUnlock()

		return registrations
	}

	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.registry) == 0 || c.registrations != nil {
		return c.registrations
	}

	registrations := make([]storage.FunctionRegistration, 0, len(c.registry))
	for key, entry := range c.registry {
		registrations = append(registrations, storage.FunctionRegistration{Key: key, Signature: entry.signature})
	}

	slices.SortFunc(registrations, func(left, right storage.FunctionRegistration) int {
		return strings.Compare(left.Key, right.Key)
	})

	c.registrations = registrations

	return c.registrations
}

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
	remaining := claim.Lease.Remaining
	if remaining <= 0 {
		remaining = c.leaseTTL
	}

	state := newHeartbeatState(remaining, c.heartbeatInterval)
	defer state.stop()

	for {
		select {
		case <-ctx.Done():
			done <- state.held

			return
		case <-state.leaseTimer.C:
			cancel()

			done <- false

			return
		case <-state.ticker.C:
			c.startHeartbeatCall(ctx, claim, state)
		case result := <-state.results:
			if !c.applyHeartbeatResult(claim, state, result) {
				cancel()

				done <- false

				return
			}
		}
	}
}

type heartbeatState struct {
	leaseTimer     *time.Timer
	ticker         *time.Ticker
	results        chan heartbeatResult
	safetyDeadline time.Time
	inFlight       bool
	held           bool
}

func newHeartbeatState(remaining, heartbeatInterval time.Duration) *heartbeatState {
	// Remaining is measured by the database. The local monotonic deadline keeps
	// lease safety independent of a heartbeat call that blocks in a SQL driver.
	safetyWindow := heartbeatSafetyWindow(remaining, heartbeatInterval)

	return &heartbeatState{
		leaseTimer:     time.NewTimer(safetyWindow),
		ticker:         time.NewTicker(heartbeatInterval),
		results:        make(chan heartbeatResult, 1),
		safetyDeadline: time.Now().Add(safetyWindow),
		held:           true,
	}
}

func (state *heartbeatState) stop() {
	state.leaseTimer.Stop()
	state.ticker.Stop()
}

func (c *Cord) startHeartbeatCall(ctx context.Context, claim *storage.Claim, state *heartbeatState) {
	if state.inFlight || !c.acquireHeartbeatCall(ctx) {
		return
	}

	state.inFlight = true
	callCtx, callCancel := context.WithDeadline(ctx, state.safetyDeadline)
	claimCopy := *claim

	go func() {
		defer c.releaseHeartbeatCall()
		defer callCancel()

		outcome, remaining := c.heartbeatOnce(callCtx, &claimCopy)
		select {
		case state.results <- heartbeatResult{outcome: outcome, remaining: remaining}:
		case <-ctx.Done():
		}
	}()
}

func (c *Cord) applyHeartbeatResult(
	claim *storage.Claim,
	state *heartbeatState,
	result heartbeatResult,
) bool {
	state.inFlight = false
	if !time.Now().Before(state.safetyDeadline) || result.outcome == heartbeatLost {
		state.held = false

		return false
	}

	if result.outcome == heartbeatRetryable {
		return true
	}

	claim.Lease.Remaining = result.remaining
	if result.remaining <= c.heartbeatInterval {
		state.held = false

		return false
	}

	safetyWindow := heartbeatSafetyWindow(result.remaining, c.heartbeatInterval)
	state.safetyDeadline = time.Now().Add(safetyWindow)
	resetTimer(state.leaseTimer, safetyWindow)

	return true
}

func (c *Cord) acquireHeartbeatCall(ctx context.Context) bool {
	c.lifecycleMu.Lock()
	if c.heartbeatCalls == nil {
		capacity := cap(c.slots)
		if capacity == 0 {
			capacity = 1
		}

		c.heartbeatCalls = make(chan struct{}, capacity)
	}

	heartbeatCalls := c.heartbeatCalls
	c.lifecycleMu.Unlock()

	select {
	case heartbeatCalls <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func (c *Cord) releaseHeartbeatCall() {
	<-c.heartbeatCalls
}

type heartbeatResult struct {
	remaining time.Duration
	outcome   heartbeatOutcome
}

type heartbeatOutcome uint8

const (
	heartbeatAccepted heartbeatOutcome = iota
	heartbeatRetryable
	heartbeatLost
)

func heartbeatSafetyWindow(remaining, retryMargin time.Duration) time.Duration {
	if remaining <= retryMargin {
		return 0
	}

	return remaining - retryMargin
}

func (c *Cord) heartbeatOnce(ctx context.Context, claim *storage.Claim) (heartbeatOutcome, time.Duration) {
	accepted, remaining, err := c.store.HeartbeatNode(ctx, claim.RunID, claim.NodeID, claim.Lease, c.leaseTTL)
	if err != nil {
		if ctx.Err() == nil {
			c.reportSchedulerError(fmt.Errorf("cord: heartbeat node: %w", err))
		}

		return heartbeatRetryable, 0
	}

	if !accepted {
		return heartbeatLost, 0
	}

	return heartbeatAccepted, remaining
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

func decodeRunError(payload storage.EncodedPayload) error {
	var failure persistedFailure
	if err := json.Unmarshal(payload, &failure); err != nil || failure.Message == "" {
		return errors.New("cord: workflow failed")
	}

	return runFailureError{failure: &failure}
}
