// Package cord composes typed Go functions into persistent workflow graphs.
package cord

import (
	"context"
	"errors"
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
