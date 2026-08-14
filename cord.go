// Package cord composes typed Go functions into persistent workflow graphs.
package cord

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

// Cord is a persistent workflow runtime. Its concurrency limit is shared by all workflows.
type Cord struct {
	ctx               context.Context
	cancel            context.CancelFunc
	store             *storage.Store
	registry          map[string]registeredInvocation
	onSchedulerError  func(error)
	wake              chan struct{}
	slots             chan struct{}
	owner             string
	registryJSON      []byte
	workers           sync.WaitGroup
	mu                sync.RWMutex
	closeOnce         sync.Once
	retry             RetryPolicy
	pollInterval      time.Duration
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
}

// Close releases resources owned by the runtime. It never closes a caller-owned database.
func (c *Cord) Close() error {
	if c == nil {
		return nil
	}

	c.closeOnce.Do(c.cancel)
	c.workers.Wait()

	return nil
}

// SetRetryPolicy sets the policy snapshotted into subsequently submitted runs.
// Existing runs retain the policy persisted when they were submitted.
func (c *Cord) SetRetryPolicy(policy RetryPolicy) error {
	if c == nil {
		return nil
	}

	if err := policy.validate(); err != nil {
		return err
	}

	c.mu.Lock()
	c.retry = policy
	c.mu.Unlock()

	return nil
}

func (c *Cord) retryPolicy() RetryPolicy {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.retry
}

func (c *Cord) reportSchedulerError(err error) {
	if c.onSchedulerError != nil && err != nil {
		c.onSchedulerError(err)
	}
}

func newRuntimeContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

type schedulerSettings struct {
	onSchedulerError  func(error)
	pollInterval      time.Duration
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
}

func newCordWithSettings(
	concurrency int,
	store *storage.Store,
	owner string,
	settings schedulerSettings,
) *Cord {
	ctx, cancel := newRuntimeContext()
	cordRuntime := &Cord{
		ctx: ctx, cancel: cancel, heartbeatInterval: settings.heartbeatInterval,
		leaseTTL: settings.leaseTTL, pollInterval: settings.pollInterval,
		onSchedulerError: settings.onSchedulerError,
		mu:               sync.RWMutex{}, workers: sync.WaitGroup{}, registry: make(map[string]registeredInvocation),
		registryJSON: nil, retry: defaultRetryPolicy(),
		slots: make(chan struct{}, max(1, concurrency)), store: store,
		wake: make(chan struct{}, 1), owner: owner, closeOnce: sync.Once{},
	}

	cordRuntime.workers.Add(1)
	go cordRuntime.scheduler()

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
	tail := graph.appendNode([]nodeID{}, adaptStep(step), definition)

	return Workflow[I, O]{runtime: c, graph: graph, tail: tail, err: registrationErr}
}
