package cord

import (
	"context"
	"errors"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

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
	leaseTimer               *time.Timer
	ticker                   *time.Ticker
	results                  chan heartbeatResult
	safetyDeadline           time.Time
	inFlight                 bool
	permitExhaustionReported bool
	held                     bool
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
	if state.inFlight {
		return
	}

	if !c.acquireHeartbeatCall(ctx) {
		if ctx.Err() == nil && !state.permitExhaustionReported {
			state.permitExhaustionReported = true

			c.reportSchedulerError(errors.New("cord: heartbeat call capacity exhausted"))
		}

		return
	}

	state.permitExhaustionReported = false
	state.inFlight = true
	deadline := state.safetyDeadline
	claimCopy := *claim

	go func() {
		defer c.releaseHeartbeatCall()

		callCtx, callCancel := context.WithDeadline(ctx, deadline)
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
