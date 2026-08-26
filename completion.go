package cord

import (
	"context"
	"sync"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

type completionObservation struct {
	err    error
	result storage.RunResult
}

type completionWaiter struct {
	observations        chan completionObservation
	observeRuntimeClose bool
}

type completionPoll struct {
	waiters map[uint64]completionWaiter
	trigger chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
	latest  *completionObservation
}

func (c *Cord) subscribeCompletion(
	runID storage.RunID,
	observeRuntimeClose bool,
) (observations <-chan completionObservation, unsubscribe func()) {
	c.waiterMu.Lock()

	poll := c.completionWaiters[runID]
	if poll == nil {
		pollCtx, cancel := context.WithCancel(context.Background())
		poll = &completionPoll{
			waiters: make(map[uint64]completionWaiter),
			trigger: make(chan struct{}, 1),
			done:    make(chan struct{}),
			cancel:  cancel,
		}
		c.completionWaiters[runID] = poll

		go c.pollCompletion(pollCtx, runID, poll)
	}

	c.nextWaiterID++
	waiterID := c.nextWaiterID
	waiter := completionWaiter{
		observations:        make(chan completionObservation, 1),
		observeRuntimeClose: observeRuntimeClose,
	}

	poll.waiters[waiterID] = waiter
	if poll.latest != nil {
		waiter.observations <- *poll.latest
	}
	c.waiterMu.Unlock()

	var unsubscribeOnce sync.Once

	return waiter.observations, func() {
		unsubscribeOnce.Do(func() {
			var waitForPoll <-chan struct{}

			c.waiterMu.Lock()
			delete(poll.waiters, waiterID)

			if len(poll.waiters) == 0 {
				if c.completionWaiters[runID] == poll {
					delete(c.completionWaiters, runID)
				}

				poll.cancel()
				waitForPoll = poll.done
			}
			c.waiterMu.Unlock()

			if waitForPoll != nil {
				<-waitForPoll
			}
		})
	}
}

func (c *Cord) pollCompletion(ctx context.Context, runID storage.RunID, poll *completionPoll) {
	defer close(poll.done)

	timer := time.NewTimer(resultPollInterval)
	defer timer.Stop()

	for {
		if c.readCompletion(ctx, runID, poll) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-poll.trigger:
		case <-timer.C:
		}

		resetTimer(timer, resultPollInterval)
	}
}

func (c *Cord) readCompletion(ctx context.Context, runID storage.RunID, poll *completionPoll) bool {
	result, err := c.store.GetRunResult(ctx, runID)
	if ctx.Err() != nil {
		return true
	}

	observation := &completionObservation{err: err, result: result}
	final := err != nil || result.Status == storage.RunCompleted ||
		result.Status == storage.RunFailed || result.Status == storage.RunCanceled
	c.publishCompletion(poll, observation, final)

	return final
}

func resetTimer(timer *time.Timer, interval time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	timer.Reset(interval)
}

func (c *Cord) notifyCompletion(runID storage.RunID) {
	// Workflow cancellation reaches this hook only after the durable transition.
	// Local cancellation is an optimization; durable fencing remains authoritative.
	c.cancelActiveAttempts(runID)
	c.notifyWaiters(runID)
}

func (c *Cord) notifyWaiters(runID storage.RunID) {
	c.waiterMu.Lock()
	poll := c.completionWaiters[runID]
	c.waiterMu.Unlock()

	if poll == nil {
		return
	}

	select {
	case poll.trigger <- struct{}{}:
	default:
	}
}

func (c *Cord) clearCompletionWaiters() {
	var stopped []*completionPoll

	c.waiterMu.Lock()
	for runID, poll := range c.completionWaiters {
		for waiterID, waiter := range poll.waiters {
			if waiter.observeRuntimeClose {
				delete(poll.waiters, waiterID)
			}
		}

		if len(poll.waiters) == 0 {
			delete(c.completionWaiters, runID)
			poll.cancel()
			stopped = append(stopped, poll)
		}
	}
	c.waiterMu.Unlock()

	for _, poll := range stopped {
		<-poll.done
	}
}
