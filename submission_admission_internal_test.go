package cord

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

type admissionTestBackend struct {
	storage.Backend
	createPanic    any
	createStarted  chan struct{}
	allowCreate    chan struct{}
	created        chan storage.RunID
	resultRead     chan struct{}
	result         resultStore
	startOnce      sync.Once
	resultReadOnce sync.Once
	attached       bool
}

func (backend *admissionTestBackend) CreateRun(ctx context.Context, plan *storage.RunPlan) error {
	_, _, err := backend.CreateOrAttachRun(ctx, plan)

	return err
}

func (backend *admissionTestBackend) CreateOrAttachRun(
	ctx context.Context,
	plan *storage.RunPlan,
) (storage.RunID, bool, error) {
	backend.startOnce.Do(func() { close(backend.createStarted) })

	if backend.createPanic != nil {
		panic(backend.createPanic)
	}

	select {
	case <-backend.allowCreate:
	case <-ctx.Done():
		return "", false, fmt.Errorf("admission test create run: %w", ctx.Err())
	}

	backend.created <- plan.Run.ID

	return plan.Run.ID, !backend.attached, nil
}

func (backend *admissionTestBackend) GetRunResult(context.Context, storage.RunID) (storage.RunResult, error) {
	result := backend.result.get()

	if backend.resultRead != nil {
		backend.resultReadOnce.Do(func() { close(backend.resultRead) })
	}

	return result, nil
}

func (*admissionTestBackend) ClaimReadyNodeForFunctions(
	context.Context,
	string,
	time.Duration,
	[]storage.FunctionRegistration,
) (*storage.Claim, bool, error) {
	return nil, false, nil
}

func newAdmissionTestRuntime(backend storage.Backend) *Cord {
	return newCordWithSettings(backend, "admission-test", schedulerSettings{
		concurrency:       1,
		pollInterval:      time.Hour,
		leaseTTL:          defaultLeaseTTL,
		heartbeatInterval: defaultHeartbeatInterval,
		retry:             defaultRetryPolicy(),
	})
}

func admissionTestStep(_ context.Context, input int) (int, error) { return input + 1, nil }

func TestWorkflowPersistRunWakesSchedulerAfterAttach(t *testing.T) {
	t.Parallel()

	allowCreate := make(chan struct{})
	close(allowCreate)

	backend := &admissionTestBackend{
		attached: true, createStarted: make(chan struct{}), allowCreate: allowCreate,
		created: make(chan storage.RunID, 1),
	}
	runtime := &Cord{
		store: backend, wake: make(chan struct{}, 1), admittedRuns: 1,
		acceptingRuns: true, admissionMu: sync.Mutex{},
	}
	workflow := Workflow[int, int]{runtime: runtime}
	plan := &storage.RunPlan{
		Nodes: nil, Edges: nil,
		Run: storage.Run{
			CreatedAt: time.Time{}, UpdatedAt: time.Time{}, CompletedAt: nil, StartedAt: nil,
			TerminalReason: nil, TerminalRunnerID: nil,
			ID: "attached-run", WorkflowName: "", DefinitionHash: "",
			IdempotencyKey: nil, SubmissionFingerprint: nil, TerminalNodeID: "",
			Status: "", Input: nil, Output: nil, Error: nil,
			MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
		},
	}

	id, _, err := workflow.persistRun(t.Context(), plan)
	require.NoError(t, err)
	require.Equal(t, storage.RunID("attached-run"), id)
	require.Len(t, runtime.wake, 1)
}

// TestWorkflowRunCreateRunPanicReleasesAdmission verifies that a CreateRun
// panic releases admission and allows shutdown to complete.
func TestWorkflowRunCreateRunPanicReleasesAdmission(t *testing.T) {
	t.Parallel()

	const panicValue = "create run panic"

	backend := &admissionTestBackend{
		createStarted: make(chan struct{}),
		createPanic:   panicValue,
	}
	runtime := newAdmissionTestRuntime(backend)
	flow := runtime.From("panic-during-create", admissionTestStep)

	panicResult := make(chan any, 1)

	go func() {
		defer func() { panicResult <- recover() }()

		if _, err := flow.Run(t.Context(), 1); err != nil {
			panic(err)
		}
	}()

	select {
	case recovered := <-panicResult:
		require.Equal(t, panicValue, recovered)
	case <-time.After(time.Second):
		t.Fatal("CreateRun panic did not propagate")
	}

	shutdownCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	require.NoError(t, runtime.Shutdown(shutdownCtx))
}
