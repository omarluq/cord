package cord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const schedulerErrorQueueCapacity = 16

type schedulerErrorsDroppedError struct {
	count uint64
}

func (err schedulerErrorsDroppedError) Error() string {
	return fmt.Sprintf("cord: %d scheduler errors dropped while OnSchedulerError was busy", err.count)
}

func (c *Cord) reportSchedulerError(err error) {
	if c.onSchedulerError == nil || err == nil || c.errorReportingOff.Load() || c.ctx.Err() != nil {
		return
	}

	select {
	case c.errorReports <- err:
	default:
		c.droppedErrors.Add(1)
	}
}

// runErrorReporter is intentionally not counted among lifecycle goroutines.
// A user callback cannot be canceled, so excluding this single, serialized
// reporter lets callbacks invoke lifecycle methods without waiting on themselves.
// Cancellation abandons queued reports; an in-flight callback exits when user
// code returns, after which this goroutine observes cancellation and terminates.
func (c *Cord) runErrorReporter() {
	defer close(c.errorReporterDone)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		select {
		case <-c.ctx.Done():
			return
		case err := <-c.errorReports:
			if c.errorReportingOff.Load() {
				return
			}

			c.invokeSchedulerErrorCallback(err)
		}

		if c.ctx.Err() != nil {
			return
		}

		if dropped := c.droppedErrors.Swap(0); dropped != 0 {
			if c.errorReportingOff.Load() {
				return
			}

			c.invokeSchedulerErrorCallback(schedulerErrorsDroppedError{count: dropped})
		}
	}
}

func (c *Cord) invokeSchedulerErrorCallback(err error) {
	defer recoverSchedulerErrorCallback()

	c.onSchedulerError(err)
}

func recoverSchedulerErrorCallback() {
	if recovered := recover(); recovered != nil {
		return
	}
}

func newRuntimeContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

type schedulerSettings struct {
	onSchedulerError  func(error)
	concurrency       int
	pollInterval      time.Duration
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
	retry             retryPolicy
}

func newCordWithSettings(store storage.Backend, owner string, settings schedulerSettings) *Cord {
	ctx, cancel := newRuntimeContext()
	cordRuntime := &Cord{
		ctx: ctx, cancel: cancel, heartbeatInterval: settings.heartbeatInterval,
		leaseTTL: settings.leaseTTL, pollInterval: settings.pollInterval,
		onSchedulerError: settings.onSchedulerError,
		mu:               sync.RWMutex{}, registry: make(map[string]registeredInvocation), registrations: nil,
		retry: settings.retry, slots: make(chan struct{}, settings.concurrency),
		heartbeatCalls: make(chan struct{}, settings.concurrency), store: store,
		wake: make(chan struct{}, 1), owner: owner, closeOnce: sync.Once{},
		lifecycleMu: sync.Mutex{}, shutdownDone: make(chan struct{}),
		admissionMu: sync.Mutex{}, acceptingRuns: true, waiterMu: sync.Mutex{},
		completionWaiters: make(map[storage.RunID]*completionPoll),
		activeAttempts:    make(map[storage.RunID]map[activeAttemptKey]*activeAttempt),
		errorReports:      make(chan error, schedulerErrorQueueCapacity), errorReporterDone: make(chan struct{}),
	}

	cordRuntime.addGoroutine()

	go cordRuntime.scheduler()

	if cordRuntime.onSchedulerError != nil {
		go cordRuntime.runErrorReporter()
	}

	return cordRuntime
}
