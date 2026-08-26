package cord

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
)

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
			return "", shutdownOverlappedPersistence, fmt.Errorf(
				"cord: persist run: %w: %w",
				ErrRunConflict,
				err,
			)
		}

		return "", shutdownOverlappedPersistence, fmt.Errorf("cord: persist run: %w", err)
	}

	w.runtime.signalScheduler()

	return id, shutdownOverlappedPersistence, nil
}

const maxIdempotencyKeyBytes = 255
