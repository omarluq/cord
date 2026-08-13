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
	wake              chan struct{}
	store             *storage.Store
	slots             chan struct{}
	registry          map[string]registeredInvocation
	failures          map[string]error
	owner             string
	retry             RetryPolicy
	workers           sync.WaitGroup
	pollInterval      time.Duration
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
	mu                sync.RWMutex
	closeOnce         sync.Once
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

// SetRetryPolicy sets the policy used for subsequently classified node failures.
// Existing persistent attempt counts and retry deadlines are preserved.
func (c *Cord) SetRetryPolicy(policy RetryPolicy) error {
	if err := policy.validate(); err != nil {
		return err
	}

	c.mu.Lock()
	c.retry = policy
	c.mu.Unlock()

	return nil
}

func newRuntimeContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

type schedulerSettings struct {
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
		mu: sync.RWMutex{}, workers: sync.WaitGroup{}, registry: make(map[string]registeredInvocation),
		failures: make(map[string]error),
		retry:    defaultRetryPolicy(), slots: make(chan struct{}, max(1, concurrency)), store: store,
		wake: make(chan struct{}, 1), owner: owner, closeOnce: sync.Once{},
	}

	cordRuntime.workers.Add(1)
	go cordRuntime.scheduler()

	return cordRuntime
}

// From creates a workflow whose root node invokes step. The root step's stable
// package-level function key is also the persisted workflow name.
func (c *Cord) From[I, O any](step func(context.Context, I) (O, error)) Workflow[I, O] {
	if step == nil {
		return Workflow[I, O]{runtime: c, graph: newGraph(""), err: errors.New("cord: root step is nil")}
	}

	definition := stepDefinition(step)
	registrationErr := c.register(definition, encodedStep(step))
	graph := newGraph(definition.functionKey)
	tail := graph.appendNode([]nodeID{}, adaptStep(step), definition)

	return Workflow[I, O]{runtime: c, graph: graph, tail: tail, err: registrationErr}
}
