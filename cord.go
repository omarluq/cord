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
	heartbeatCalls    chan struct{}
	errorReports      chan error
	errorReporterDone chan struct{}
	completionWaiters map[storage.RunID]*completionPoll
	activeAttempts    map[storage.RunID]map[activeAttemptKey]*activeAttempt
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
	activeMu          sync.Mutex
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
// It reports whether shutdown began while the persistence attempt was admitted.
func (c *Cord) finishRunAdmission() bool {
	c.admissionMu.Lock()
	c.admittedRuns--
	shutdownStarted := !c.acceptingRuns
	stopNow := shutdownStarted && c.admittedRuns == 0
	c.admissionMu.Unlock()

	if stopNow {
		c.stopRuntime()
	}

	return shutdownStarted
}

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

func (c *Cord) publishCompletion(
	poll *completionPoll,
	observation *completionObservation,
	final bool,
) {
	c.waiterMu.Lock()
	fanout := final || poll.latest == nil
	poll.latest = observation

	waiters := make([]chan completionObservation, 0, len(poll.waiters))
	if fanout {
		for _, waiter := range poll.waiters {
			waiters = append(waiters, waiter.observations)
		}
	}
	c.waiterMu.Unlock()

	for _, waiter := range waiters {
		publishCompletionToWaiter(waiter, observation)
	}
}

func publishCompletionToWaiter(
	waiter chan completionObservation,
	observation *completionObservation,
) {
	select {
	case waiter <- *observation:
		return
	default:
	}

	select {
	case <-waiter:
	default:
	}

	select {
	case waiter <- *observation:
	default:
	}
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

type activeAttemptKey struct {
	runID      storage.RunID
	nodeID     storage.NodeID
	leaseOwner string
	generation int64
}

type activeAttempt struct {
	cancel context.CancelFunc
}

func newActiveAttemptKey(claim *storage.Claim) activeAttemptKey {
	return activeAttemptKey{
		runID:      claim.RunID,
		nodeID:     claim.NodeID,
		leaseOwner: claim.Lease.Owner,
		generation: claim.Lease.Generation,
	}
}

func (c *Cord) registerActiveAttemptLocked(
	claim *storage.Claim,
	cancel context.CancelFunc,
) (unregister func()) {
	key := newActiveAttemptKey(claim)
	attempt := &activeAttempt{cancel: cancel}

	if c.activeAttempts == nil {
		c.activeAttempts = make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)
	}

	if c.activeAttempts[claim.RunID] == nil {
		c.activeAttempts[claim.RunID] = make(map[activeAttemptKey]*activeAttempt)
	}

	if previous := c.activeAttempts[claim.RunID][key]; previous != nil {
		previous.cancel()
	}

	c.activeAttempts[claim.RunID][key] = attempt

	return func() {
		c.activeMu.Lock()
		defer c.activeMu.Unlock()

		attempts := c.activeAttempts[claim.RunID]
		if attempts[key] != attempt {
			return
		}

		delete(attempts, key)

		if len(attempts) == 0 {
			delete(c.activeAttempts, claim.RunID)
		}
	}
}

func (c *Cord) cancelActiveAttempts(runID storage.RunID) {
	c.activeMu.Lock()
	attempts := c.activeAttempts[runID]
	delete(c.activeAttempts, runID)
	c.activeMu.Unlock()

	for _, attempt := range attempts {
		attempt.cancel()
	}
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
