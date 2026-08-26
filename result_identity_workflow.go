package cord

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
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
