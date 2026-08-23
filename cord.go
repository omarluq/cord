// Package cord composes typed Go functions into persistent workflow graphs.
package cord

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// Cord is a persistent workflow runtime. Its concurrency limit is shared by all workflows.
type Cord struct {
	store             storage.Backend
	ctx               context.Context
	wake              chan struct{}
	cancel            context.CancelFunc
	registry          map[string]registeredInvocation
	onSchedulerError  func(error)
	slots             chan struct{}
	errorReports      chan error
	errorReporterDone chan struct{}
	completionWaiters map[storage.RunID]map[uint64]chan struct{}
	shutdownDone      chan struct{}
	owner             string
	registrations     []storage.FunctionRegistration
	retry             retryPolicy
	admittedRuns      int
	activeGoroutines  int
	nextWaiterID      uint64
	pollInterval      time.Duration
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
	droppedErrors     atomic.Uint64
	mu                sync.RWMutex
	closeOnce         sync.Once
	lifecycleMu       sync.Mutex
	admissionMu       sync.Mutex
	waiterMu          sync.Mutex
	errorReportingOff atomic.Bool
	acceptingRuns     bool
}

// Close releases resources owned by the runtime. It waits for executing steps
// to return and never closes a caller-owned database. Scheduler error callbacks
// are not part of this wait and may call Close safely. Steps must observe their
// context. Call Shutdown when the caller needs a bounded wait.
func (c *Cord) Close() error {
	return c.Shutdown(context.Background())
}

// Shutdown requests runtime cancellation and waits until its scheduler and
// executing steps exit or ctx is done. It does not wait for scheduler error
// callbacks, so callbacks may call Shutdown safely and a blocked callback may
// outlive the wait. It does not close the caller-owned database.
func (c *Cord) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}

	if ctx == nil {
		return errors.New("cord: shutdown context is nil")
	}

	c.closeOnce.Do(c.beginShutdown)

	select {
	case <-c.shutdownDone:
		return nil
	default:
	}

	select {
	case <-c.shutdownDone:
		return nil
	case <-ctx.Done():
		select {
		case <-c.shutdownDone:
			return nil
		default:
			return fmt.Errorf("cord: wait for shutdown: %w", ctx.Err())
		}
	}
}

// beginShutdown is the shutdown linearization point for submissions. It closes
// admission before canceling runtime work, so later submissions cannot persist.
func (c *Cord) beginShutdown() {
	c.errorReportingOff.Store(true)

	c.admissionMu.Lock()
	c.acceptingRuns = false
	stopNow := c.admittedRuns == 0
	c.admissionMu.Unlock()

	if stopNow {
		c.stopRuntime()
	}
}

// stopRuntime cancels local execution only after every admitted submission has
// finished its persistence attempt. Durable work itself is never canceled.
func (c *Cord) stopRuntime() {
	c.cancel()
	c.clearCompletionWaiters()
}

// admitRun is the submission linearization point. Admission is never revoked:
// once won, a submission may finish persistence after shutdown begins.
func (c *Cord) admitRun() bool {
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()

	if !c.acceptingRuns {
		return false
	}

	c.admittedRuns++

	return true
}

// finishRunAdmission releases admission after persistence and lets a waiting
// shutdown cancel local execution once all admitted submissions are reported.
func (c *Cord) finishRunAdmission() {
	c.admissionMu.Lock()
	c.admittedRuns--
	stopNow := !c.acceptingRuns && c.admittedRuns == 0
	c.admissionMu.Unlock()

	if stopNow {
		c.stopRuntime()
	}
}

func (c *Cord) subscribeCompletion(
	runID storage.RunID,
) (notifications <-chan struct{}, unsubscribe func()) {
	c.waiterMu.Lock()
	defer c.waiterMu.Unlock()

	c.nextWaiterID++
	waiterID := c.nextWaiterID
	waiter := make(chan struct{}, 1)

	if c.completionWaiters[runID] == nil {
		c.completionWaiters[runID] = make(map[uint64]chan struct{})
	}

	c.completionWaiters[runID][waiterID] = waiter

	return waiter, func() {
		c.waiterMu.Lock()
		defer c.waiterMu.Unlock()

		waiters := c.completionWaiters[runID]
		delete(waiters, waiterID)

		if len(waiters) == 0 {
			delete(c.completionWaiters, runID)
		}
	}
}

func (c *Cord) notifyCompletion(runID storage.RunID) {
	c.waiterMu.Lock()
	defer c.waiterMu.Unlock()

	for _, waiter := range c.completionWaiters[runID] {
		select {
		case waiter <- struct{}{}:
		default:
		}
	}
}

func (c *Cord) clearCompletionWaiters() {
	c.waiterMu.Lock()
	defer c.waiterMu.Unlock()

	clear(c.completionWaiters)
}

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

func (c *Cord) addGoroutine() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.activeGoroutines++
}

func (c *Cord) goroutineDone() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.activeGoroutines--
	if c.activeGoroutines == 0 {
		close(c.shutdownDone)
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
		retry: settings.retry, slots: make(chan struct{}, settings.concurrency), store: store,
		wake: make(chan struct{}, 1), owner: owner, closeOnce: sync.Once{},
		lifecycleMu: sync.Mutex{}, shutdownDone: make(chan struct{}),
		admissionMu: sync.Mutex{}, acceptingRuns: true, waiterMu: sync.Mutex{},
		completionWaiters: make(map[storage.RunID]map[uint64]chan struct{}),
		errorReports:      make(chan error, schedulerErrorQueueCapacity), errorReporterDone: make(chan struct{}),
	}

	cordRuntime.addGoroutine()

	go cordRuntime.scheduler()

	if cordRuntime.onSchedulerError != nil {
		go cordRuntime.runErrorReporter()
	}

	return cordRuntime
}

// From creates a named workflow whose root node invokes step. Name is the
// workflow's durable identity and must remain stable across implementations.
func (c *Cord) From[I, O any](name string, step func(context.Context, I) (O, error)) Workflow[I, O] {
	graph := newGraph(name)

	if name == "" {
		return Workflow[I, O]{runtime: c, graph: graph, err: errors.New("cord: workflow name is empty")}
	}

	if step == nil {
		return Workflow[I, O]{runtime: c, graph: graph, err: errors.New("cord: root step is nil")}
	}

	definition := stepDefinition(step)
	registrationErr := c.register(definition, encodedStep(step))
	tail := graph.appendNode([]nodeID{}, definition)

	return Workflow[I, O]{runtime: c, graph: graph, tail: tail, err: registrationErr}
}
