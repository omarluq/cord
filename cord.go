// Package cord composes typed Go functions into persistent workflow graphs.
package cord

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// Cord is a persistent workflow runtime. Its concurrency limit is shared by all workflows.
type Cord struct {
	store             storage.Backend
	ctx               context.Context
	shutdownDone      chan struct{}
	cancel            context.CancelFunc
	registry          map[string]registeredInvocation
	onSchedulerError  func(error)
	wake              chan struct{}
	slots             chan struct{}
	errorReports      chan struct{}
	pendingError      error
	owner             string
	registrations     []storage.FunctionRegistration
	retry             retryPolicy
	activeGoroutines  int
	pollInterval      time.Duration
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
	mu                sync.RWMutex
	errorMu           sync.Mutex
	closeOnce         sync.Once
	lifecycleMu       sync.Mutex
}

// Close releases resources owned by the runtime. It waits for executing steps
// and scheduler error callbacks to return, and never closes a caller-owned database.
// Steps must observe their context and callbacks must return promptly. Call
// Shutdown when the caller needs a bounded wait.
func (c *Cord) Close() error {
	return c.Shutdown(context.Background())
}

// Shutdown requests runtime cancellation and waits until its goroutines exit or
// ctx is done. It does not close the caller-owned database. A step that ignores
// cancellation, or a scheduler error callback that blocks, can outlive the wait.
func (c *Cord) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}

	if ctx == nil {
		return errors.New("cord: shutdown context is nil")
	}

	c.closeOnce.Do(c.cancel)

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

func (c *Cord) reportSchedulerError(err error) {
	if c.onSchedulerError == nil || err == nil {
		return
	}

	c.errorMu.Lock()
	c.pendingError = errors.Join(c.pendingError, err)
	c.errorMu.Unlock()

	select {
	case c.errorReports <- struct{}{}:
	default:
	}
}

func (c *Cord) runErrorReporter() {
	defer c.goroutineDone()

	for {
		select {
		case <-c.ctx.Done():
			c.deliverSchedulerErrors()

			return
		case <-c.errorReports:
			c.deliverSchedulerErrors()
		}
	}
}

func (c *Cord) deliverSchedulerErrors() {
	c.errorMu.Lock()
	err := c.pendingError
	c.pendingError = nil
	c.errorMu.Unlock()

	if err != nil {
		c.onSchedulerError(err)
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
		lifecycleMu: sync.Mutex{}, errorMu: sync.Mutex{}, shutdownDone: make(chan struct{}),
		errorReports: make(chan struct{}, 1),
	}

	cordRuntime.addGoroutine()

	if cordRuntime.onSchedulerError != nil {
		cordRuntime.addGoroutine()
	}

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
