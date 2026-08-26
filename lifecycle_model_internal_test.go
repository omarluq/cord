package cord

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLifecycleModelProperties(t *testing.T) {
	t.Parallel()

	runStates := []RunState{
		RunStateRunning, RunStateCanceling, RunStateCompleted, RunStateFailed, RunStateCanceled,
	}
	nodeStates := []NodeState{
		NodeStatePending, NodeStateReady, NodeStateRunning, NodeStateRetryWait,
		NodeStateCompleted, NodeStateFailed, NodeStateCanceled,
	}
	reasons := []TerminalReason{
		"", ReasonSucceeded, ReasonCanceledByRequest, ReasonCanceledByRunFailure,
		ReasonFailureNonRetryable, ReasonFailureAttemptsExhausted,
		ReasonFailureLeaseExpired, "future",
	}

	for _, state := range runStates {
		terminal, known := state.Terminal()
		assert.True(t, known)
		assert.Equal(t, state.IsKnown(), known)

		for _, reason := range reasons {
			if state.AllowsReason(reason) {
				assert.Equal(t, terminal, reason != "", "%s/%s", state, reason)
				assert.True(t, reason == "" || reason.IsKnown(), "%s/%s", state, reason)
			}
		}
	}

	for _, state := range nodeStates {
		terminal, known := state.Terminal()
		assert.True(t, known)
		assert.Equal(t, state.IsKnown(), known)

		for _, reason := range reasons {
			if state.AllowsReason(reason) {
				assert.Equal(t, terminal, reason != "", "%s/%s", state, reason)
				assert.True(t, reason == "" || reason.IsKnown(), "%s/%s", state, reason)
			}
		}
	}
}
