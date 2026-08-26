package cord

import (
	"context"
	"sync"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

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
