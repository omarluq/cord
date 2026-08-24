package cord

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
)

// Workflow is an immutable typed handle to a terminal node in a workflow graph.
type Workflow[I, O any] struct {
	runtime *Cord
	graph   *graph
	err     error
	tail    nodeID
}

// Then appends fn after the workflow's current terminal node and returns a new handle.
func (w Workflow[I, O]) Then[N any](
	step func(context.Context, O) (N, error),
) Workflow[I, N] {
	if w.err != nil {
		return Workflow[I, N](w)
	}

	if w.runtime == nil || w.graph == nil {
		return Workflow[I, N]{
			runtime: nil,
			graph:   nil,
			tail:    0,
			err:     errors.New("cord: invalid workflow"),
		}
	}

	if step == nil {
		return Workflow[I, N]{
			runtime: w.runtime,
			graph:   w.graph,
			tail:    w.tail,
			err:     errors.New("cord: workflow step is nil"),
		}
	}

	definition := stepDefinition(step)
	registrationErr := w.runtime.register(definition, encodedStep(step))
	tail := w.graph.appendNode([]nodeID{w.tail}, definition)

	return Workflow[I, N]{
		runtime: w.runtime,
		graph:   w.graph,
		tail:    tail,
		err:     errors.Join(w.err, registrationErr),
	}
}

// Run submits the workflow and waits for its terminal result. The context
// controls submission and waiting; canceling it does not cancel the durable run.
func (w Workflow[I, O]) Run(ctx context.Context, input I) (O, error) {
	var zero O

	submission, err := w.submit(ctx, input, nil)
	if err != nil {
		return zero, err
	}

	// A submission that won admission and persisted during shutdown remains
	// observable through its caller's context even after local execution stops.
	return w.wait(ctx, submission.id, submission.resultCodec, !submission.shutdownOverlappedPersistence)
}

// Submit durably submits the workflow and returns its Cord-generated UUIDv7 ID.
// At most one caller-retained idempotency key may be supplied. Reusing a retained
// key for the same workflow definition and exact encoded input returns the
// existing ID; conflicting reuse returns an error matching ErrRunConflict.
func (w Workflow[I, O]) Submit(ctx context.Context, input I, idempotencyKey ...string) (RunID, error) {
	if ctx == nil {
		return "", errors.New("cord: workflow context is nil")
	}

	key, keyed, err := validateIdempotencyKey(idempotencyKey)
	if err != nil {
		return "", err
	}

	var keyPointer *string
	if keyed {
		keyPointer = &key
	}

	submission, err := w.submit(ctx, input, keyPointer)
	if err != nil {
		return "", err
	}

	return RunID(submission.id), nil
}

type preparedSubmission[O any] struct {
	resultCodec                   serialization.JSONCodec[O]
	id                            storage.RunID
	shutdownOverlappedPersistence bool
}

func (w Workflow[I, O]) submit(
	ctx context.Context,
	input I,
	idempotencyKey *string,
) (preparedSubmission[O], error) {
	var zero preparedSubmission[O]

	if ctx == nil {
		return zero, errors.New("cord: workflow context is nil")
	}

	if w.err != nil {
		return zero, w.err
	}

	if w.runtime == nil || w.graph == nil {
		return zero, errors.New("cord: invalid workflow")
	}

	runPlan, resultCodec, err := w.prepareSubmission(input, idempotencyKey)
	if err != nil {
		return zero, err
	}

	if !w.runtime.admitRun() {
		return zero, errors.New("cord: runtime closed")
	}

	runID, shutdownOverlappedPersistence, err := w.persistRun(ctx, runPlan)
	if err != nil {
		return zero, err
	}

	return preparedSubmission[O]{
		id: runID, resultCodec: resultCodec,
		shutdownOverlappedPersistence: shutdownOverlappedPersistence,
	}, nil
}

func (w Workflow[I, O]) prepareSubmission(
	input I,
	idempotencyKey *string,
) (*storage.RunPlan, serialization.JSONCodec[O], error) {
	var zero serialization.JSONCodec[O]

	plan, err := w.graph.compile(w.tail)
	if err != nil {
		return nil, zero, err
	}

	runPlan, err := buildPlan(w.graph.name, plan, w.tail, input, w.runtime.retry)
	if err != nil {
		return nil, zero, err
	}

	resultCodec, err := serialization.NewJSONCodec[O]()
	if err != nil {
		return nil, zero, fmt.Errorf("cord: construct result codec: %w", err)
	}

	if idempotencyKey != nil {
		fingerprint := submissionFingerprint(runPlan.Run.DefinitionHash, runPlan.Run.Input)
		runPlan.Run.IdempotencyKey = idempotencyKey
		runPlan.Run.SubmissionFingerprint = &fingerprint
	}

	return runPlan, resultCodec, nil
}

func validateIdempotencyKey(keys []string) (key string, keyed bool, err error) {
	if len(keys) > 1 {
		return "", false, errors.New("cord: Submit accepts at most one idempotency key")
	}

	if len(keys) == 0 {
		return "", false, nil
	}

	key = keys[0]
	switch {
	case key == "":
		return "", false, errors.New("cord: idempotency key is empty")
	case !utf8.ValidString(key):
		return "", false, errors.New("cord: idempotency key is not valid UTF-8")
	case strings.IndexByte(key, 0) >= 0:
		return "", false, errors.New("cord: idempotency key contains NUL")
	case len(key) > maxIdempotencyKeyBytes:
		return "", false, fmt.Errorf("cord: idempotency key is longer than %d bytes", maxIdempotencyKeyBytes)
	}

	return key, true, nil
}

func (w Workflow[I, O]) persistRun(
	ctx context.Context,
	runPlan *storage.RunPlan,
) (id storage.RunID, shutdownOverlappedPersistence bool, err error) {
	defer func() {
		shutdownOverlappedPersistence = w.runtime.finishRunAdmission()
	}()

	id, _, err = w.runtime.store.CreateOrAttachRun(ctx, runPlan)
	if err != nil {
		if errors.Is(err, storage.ErrRunConflict) {
			return "", false, fmt.Errorf("cord: persist run: %w: %w", ErrRunConflict, err)
		}

		return "", false, fmt.Errorf("cord: persist run: %w", err)
	}

	w.runtime.signalScheduler()

	return id, shutdownOverlappedPersistence, nil
}

const (
	resultPollInterval     = 100 * time.Millisecond
	maxIdempotencyKeyBytes = 255
)

// Get blocks until runID reaches a terminal durable state and returns its typed
// result. The handle must reconstruct the run's workflow name, input type,
// reachable topology, function identities and signatures, and terminal node; the
// run's persisted retry policy is used for this definition check. Canceling ctx
// stops only this wait and does not cancel the run. Missing, canceled, and
// incompatible runs return errors matching ErrRunNotFound, ErrRunCanceled, and
// ErrRunIncompatible.
func (w Workflow[I, O]) Get(ctx context.Context, runID RunID) (O, error) {
	var zero O

	if ctx == nil {
		return zero, errors.New("cord: workflow context is nil")
	}

	if runID == "" {
		return zero, errors.New("cord: run ID is empty")
	}

	identity, codec, err := w.resultIdentity()
	if err != nil {
		return zero, err
	}

	return w.waitCompatible(ctx, storage.RunID(runID), codec, &identity)
}

type resultIdentity struct {
	workflowName      string
	inputFingerprint  string
	terminal          storage.NodeID
	terminalSignature string
	nodes             []storage.Node
	edges             []storage.Edge
}

func (w Workflow[I, O]) resultIdentity() (resultIdentity, serialization.JSONCodec[O], error) {
	var codec serialization.JSONCodec[O]

	if err := w.validateForResult(); err != nil {
		return resultIdentity{}, codec, err
	}

	plan, err := w.graph.compile(w.tail)
	if err != nil {
		return resultIdentity{}, codec, err
	}

	codec, err = serialization.NewJSONCodec[O]()
	if err != nil {
		return resultIdentity{}, codec, fmt.Errorf("cord: construct result codec: %w", err)
	}

	inputFingerprint, err := typeFingerprint[I]()
	if err != nil {
		return resultIdentity{}, codec, err
	}

	nodes, edges, logicalByRuntimeID, err := topology(plan, "", time.Time{})
	if err != nil {
		return resultIdentity{}, codec, err
	}

	terminal, ok := logicalByRuntimeID[w.tail]
	if !ok {
		return resultIdentity{}, codec, errors.New("cord: workflow terminal node is missing")
	}

	return resultIdentity{
		workflowName: w.graph.name, inputFingerprint: inputFingerprint,
		terminal: terminal, terminalSignature: terminalSignature(plan, w.tail),
		nodes: nodes, edges: edges,
	}, codec, nil
}

func (w Workflow[I, O]) validateForResult() error {
	if w.err != nil {
		return w.err
	}

	if w.runtime == nil || w.graph == nil {
		return errors.New("cord: invalid workflow")
	}

	return nil
}

func terminalSignature(plan []node, terminal nodeID) string {
	for index := range plan {
		if plan[index].id == terminal {
			return plan[index].definition.signature
		}
	}

	return ""
}

func (w Workflow[I, O]) waitCompatible(
	ctx context.Context,
	runID storage.RunID,
	codec serialization.JSONCodec[O],
	identity *resultIdentity,
) (O, error) {
	return w.waitResult(ctx, runID, codec, true, func(result *storage.RunResult) error {
		retry := retryPolicy{
			maxAttempts: result.MaxAttempts,
			baseDelay:   result.RetryBaseDelay,
			maxDelay:    result.RetryMaxDelay,
		}

		expectedHash := ""
		if result.RetryPolicyVersion == retryPolicyVersion && retry.validate() == nil {
			expectedHash = definitionHash(
				identity.workflowName, identity.inputFingerprint, identity.terminal,
				identity.nodes, identity.edges, retry,
			)
		}

		if result.WorkflowName != identity.workflowName ||
			result.TerminalSignatureHash != identity.terminalSignature ||
			result.DefinitionHash != expectedHash {
			return fmt.Errorf(
				"%w: run %q has workflow definition %q",
				ErrRunIncompatible, runID, result.DefinitionHash,
			)
		}

		return nil
	})
}

func (w Workflow[I, O]) wait(
	ctx context.Context,
	runID storage.RunID,
	codec serialization.JSONCodec[O],
	observeRuntimeClose bool,
) (O, error) {
	return w.waitResult(ctx, runID, codec, observeRuntimeClose, nil)
}

func (w Workflow[I, O]) waitResult(
	ctx context.Context,
	runID storage.RunID,
	codec serialization.JSONCodec[O],
	observeRuntimeClose bool,
	verify func(*storage.RunResult) error,
) (O, error) {
	var zero O

	observations, unsubscribe := w.runtime.subscribeCompletion(runID, observeRuntimeClose)
	defer unsubscribe()

	for {
		observation, err := waitForResultObservation(
			ctx, w.runtime.ctx, observations, observeRuntimeClose,
		)
		if err != nil {
			return zero, err
		}

		if readErr := publicResultReadError(ctx, observation.err); readErr != nil {
			return zero, readErr
		}

		value, done, resultErr := w.verifiedResult(&observation.result, codec, verify)
		if done {
			return value, resultErr
		}
	}
}

func publicResultReadError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	if ctx.Err() != nil {
		return fmt.Errorf("cord: workflow context: %w", ctx.Err())
	}

	if errors.Is(err, storage.ErrRunNotFound) {
		return fmt.Errorf("cord: read run result: %w", ErrRunNotFound)
	}

	return fmt.Errorf("cord: read run result: %w", err)
}

func (w Workflow[I, O]) verifiedResult(
	result *storage.RunResult,
	codec serialization.JSONCodec[O],
	verify func(*storage.RunResult) error,
) (value O, done bool, err error) {
	var zero O

	if verify != nil {
		if err := verify(result); err != nil {
			return zero, true, err
		}
	}

	return w.result(result, codec)
}

func waitForResultObservation(
	ctx context.Context,
	runtimeCtx context.Context,
	observations <-chan completionObservation,
	observeRuntimeClose bool,
) (completionObservation, error) {
	var runtimeDone <-chan struct{}
	if observeRuntimeClose {
		runtimeDone = runtimeCtx.Done()
	}

	select {
	case <-runtimeDone:
		return completionObservation{}, errors.New("cord: runtime closed")
	case <-ctx.Done():
		return completionObservation{}, fmt.Errorf("cord: workflow context: %w", ctx.Err())
	case observation := <-observations:
		return observation, nil
	}
}

func (w Workflow[I, O]) result(
	result *storage.RunResult,
	codec serialization.JSONCodec[O],
) (value O, done bool, err error) {
	var zero O

	switch result.Status {
	case storage.RunCompleted:
		value, decodeErr := codec.Decode(result.Output)
		if decodeErr != nil {
			return zero, true, fmt.Errorf("cord: decode terminal workflow output: %w", decodeErr)
		}

		return value, true, nil
	case storage.RunFailed:
		return zero, true, decodeRunError(result.Error)
	case storage.RunCanceled:
		return zero, true, ErrRunCanceled
	case storage.RunRunning, storage.RunCanceling:
		return zero, false, nil
	}

	return zero, true, fmt.Errorf("cord: unknown workflow run status %q", result.Status)
}

// Cancel durably cancels runID without checking this handle's workflow identity.
// Callers must authorize runID before calling Cancel. Cancellation is idempotent
// for an already canceled run; missing and finished runs return errors matching
// ErrRunNotFound and ErrRunFinished. It cannot forcibly stop non-cooperative user
// code already executing. Active attempts in this runtime are signaled promptly;
// attempts in other runtimes observe cancellation through lease heartbeat failure.
func (w Workflow[I, O]) Cancel(ctx context.Context, runID RunID) error {
	if ctx == nil {
		return errors.New("cord: workflow context is nil")
	}

	if runID == "" {
		return errors.New("cord: run ID is empty")
	}

	if w.err != nil {
		return w.err
	}

	if w.runtime == nil || w.graph == nil {
		return errors.New("cord: invalid workflow")
	}

	outcome, err := w.runtime.store.CancelRun(ctx, storage.RunID(runID))
	if err != nil {
		return fmt.Errorf("cord: cancel run: %w", err)
	}

	return w.cancellationResult(storage.RunID(runID), outcome)
}

func (w Workflow[I, O]) cancellationResult(
	runID storage.RunID,
	outcome storage.CancellationOutcome,
) error {
	switch outcome {
	case storage.CancellationCanceled, storage.CancellationAlreadyCanceled:
		w.runtime.notifyCompletion(runID)

		return nil
	case storage.CancellationFinished:
		return ErrRunFinished
	case storage.CancellationNotFound:
		return ErrRunNotFound
	default:
		return fmt.Errorf("cord: unknown run cancellation outcome %q", outcome)
	}
}
