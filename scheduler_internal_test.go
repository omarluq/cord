package cord

import (
	"context"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func maintenanceTestStep(_ context.Context, value int) (int, error) {
	return value, nil
}

type heartbeatTestBackend struct {
	storage.Backend
	err       error
	called    chan struct{}
	remaining time.Duration
	accepted  bool
}

func startErrorReporterForTest(t *testing.T, runtime *Cord) {
	t.Helper()

	runtime.ctx, runtime.cancel = context.WithCancel(context.Background())
	runtime.errorReports = make(chan error, schedulerErrorQueueCapacity)

	runtime.errorReporterDone = make(chan struct{})
	go runtime.runErrorReporter()

	t.Cleanup(func() {
		runtime.cancel()

		select {
		case <-runtime.errorReporterDone:
		case <-time.After(time.Second):
			t.Error("scheduler error reporter did not exit")
		}
	})
}

func nonblockingErrorCallback(reports chan<- error) func(error) {
	return func(err error) {
		select {
		case reports <- err:
		default:
		}
	}
}

func newSchedulerCallbackRuntime(t *testing.T, callback func(error)) *Cord {
	t.Helper()

	runtime := newCordWithSettings(nil, "callback-owner", schedulerSettings{
		concurrency: 1, pollInterval: time.Hour, leaseTTL: defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval, retry: defaultRetryPolicy(),
		onSchedulerError: callback,
	})

	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	return runtime
}

const (
	completeTransition = "complete"
	failTransition     = "fail"
	retryTransition    = "retry"
	raceRunID          = "race-run"
	raceNodeID         = "race-node"
	raceOwner          = "race-owner"
)

type rejectedTransitionBackend struct {
	storage.Backend
	resultErr      error
	transitionErr  error
	transition     string
	terminalReason storage.TerminalReason
	result         storage.RunResult
}

func (backend *rejectedTransitionBackend) CompleteNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	storage.EncodedPayload,
) (bool, error) {
	backend.transition = completeTransition

	return false, backend.transitionErr
}

func (backend *rejectedTransitionBackend) FailNode(
	_ context.Context,
	_ storage.RunID,
	_ storage.NodeID,
	_ storage.Lease,
	_ storage.EncodedPayload,
	reason storage.TerminalReason,
) (bool, error) {
	backend.transition = failTransition
	backend.terminalReason = reason

	return false, backend.transitionErr
}

func (backend *rejectedTransitionBackend) RetryNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	storage.EncodedPayload,
	time.Duration,
) (bool, error) {
	backend.transition = retryTransition

	return false, backend.transitionErr
}

func (backend *rejectedTransitionBackend) GetRunResult(
	context.Context,
	storage.RunID,
) (storage.RunResult, error) {
	return backend.result, backend.resultErr
}

func (backend heartbeatTestBackend) HeartbeatNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	time.Duration,
) (bool, time.Duration, error) {
	select {
	case backend.called <- struct{}{}:
	default:
	}

	return backend.accepted, backend.remaining, backend.err
}
