package cord_test

import (
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
)

const (
	completedState = "completed"
	failedState    = "failed"
	readyState     = "ready"
)

func TestLifecycleVocabulary(t *testing.T) {
	t.Parallel()

	runStates := []struct {
		state    cord.RunState
		value    string
		terminal bool
	}{
		{state: cord.RunStateRunning, value: "running", terminal: false},
		{state: cord.RunStateCanceling, value: "canceling", terminal: false},
		{state: cord.RunStateCompleted, value: completedState, terminal: true},
		{state: cord.RunStateFailed, value: failedState, terminal: true},
		{state: cord.RunStateCanceled, value: "canceled", terminal: true},
	}
	for _, testCase := range runStates {
		assert.Equal(t, testCase.value, string(testCase.state))
		assert.True(t, testCase.state.IsKnown())
		terminal, known := testCase.state.Terminal()
		assert.True(t, known)
		assert.Equal(t, testCase.terminal, terminal)
	}

	nodeStates := []struct {
		state    cord.NodeState
		value    string
		terminal bool
	}{
		{state: cord.NodeStatePending, value: "pending", terminal: false},
		{state: cord.NodeStateReady, value: readyState, terminal: false},
		{state: cord.NodeStateRunning, value: "running", terminal: false},
		{state: cord.NodeStateRetryWait, value: "retry_wait", terminal: false},
		{state: cord.NodeStateCompleted, value: completedState, terminal: true},
		{state: cord.NodeStateFailed, value: failedState, terminal: true},
		{state: cord.NodeStateCanceled, value: "canceled", terminal: true},
	}
	for _, testCase := range nodeStates {
		assert.Equal(t, testCase.value, string(testCase.state))
		assert.True(t, testCase.state.IsKnown())
		terminal, known := testCase.state.Terminal()
		assert.True(t, known)
		assert.Equal(t, testCase.terminal, terminal)
	}

	reasons := []struct {
		reason cord.TerminalReason
		value  string
	}{
		{reason: cord.ReasonSucceeded, value: "succeeded"},
		{reason: cord.ReasonCanceledByRequest, value: "canceled_by_request"},
		{reason: cord.ReasonCanceledByRunFailure, value: "canceled_by_run_failure"},
		{reason: cord.ReasonFailureNonRetryable, value: "failure_non_retryable"},
		{reason: cord.ReasonFailureAttemptsExhausted, value: "failure_attempts_exhausted"},
		{reason: cord.ReasonFailureLeaseExpired, value: "failure_lease_expired"},
		{reason: cord.ReasonLegacyUnknown, value: "legacy_unknown"},
	}
	for _, testCase := range reasons {
		assert.Equal(t, testCase.value, string(testCase.reason))
		assert.True(t, testCase.reason.IsKnown())
	}
}

func TestLifecycleUnknownValuesFailClosed(t *testing.T) {
	t.Parallel()

	for _, state := range []cord.RunState{"", "queued", "COMPLETED"} {
		assert.False(t, state.IsKnown())
		terminal, known := state.Terminal()
		assert.False(t, terminal)
		assert.False(t, known)
		assert.False(t, state.AllowsReason(""))
		assert.False(t, state.AllowsReason(cord.ReasonSucceeded))
	}

	for _, state := range []cord.NodeState{"", "blocked", "COMPLETED"} {
		assert.False(t, state.IsKnown())
		terminal, known := state.Terminal()
		assert.False(t, terminal)
		assert.False(t, known)
		assert.False(t, state.AllowsReason(""))
		assert.False(t, state.AllowsReason(cord.ReasonSucceeded))
	}

	for _, reason := range []cord.TerminalReason{"", "unknown", "SUCCEEDED"} {
		assert.False(t, reason.IsKnown())
	}
}

func TestRunStateReasonPairs(t *testing.T) {
	t.Parallel()

	states := []cord.RunState{
		cord.RunStateRunning,
		cord.RunStateCanceling,
		cord.RunStateCompleted,
		cord.RunStateFailed,
		cord.RunStateCanceled,
	}
	reasons := allReasons()
	legal := map[cord.RunState]map[cord.TerminalReason]bool{
		cord.RunStateRunning:   {"": true},
		cord.RunStateCanceling: {"": true},
		cord.RunStateCompleted: {cord.ReasonSucceeded: true},
		cord.RunStateFailed: {
			cord.ReasonFailureNonRetryable:      true,
			cord.ReasonFailureAttemptsExhausted: true,
			cord.ReasonFailureLeaseExpired:      true,
			cord.ReasonLegacyUnknown:            true,
		},
		cord.RunStateCanceled: {cord.ReasonCanceledByRequest: true},
	}

	for _, state := range states {
		for _, reason := range reasons {
			assert.Equal(t, legal[state][reason], state.AllowsReason(reason), "%s/%s", state, reason)
		}
	}
}

func TestNodeStateReasonPairs(t *testing.T) {
	t.Parallel()

	states := []cord.NodeState{
		cord.NodeStatePending,
		cord.NodeStateReady,
		cord.NodeStateRunning,
		cord.NodeStateRetryWait,
		cord.NodeStateCompleted,
		cord.NodeStateFailed,
		cord.NodeStateCanceled,
	}
	reasons := allReasons()
	legal := map[cord.NodeState]map[cord.TerminalReason]bool{
		cord.NodeStatePending:   {"": true},
		cord.NodeStateReady:     {"": true},
		cord.NodeStateRunning:   {"": true},
		cord.NodeStateRetryWait: {"": true},
		cord.NodeStateCompleted: {cord.ReasonSucceeded: true},
		cord.NodeStateFailed: {
			cord.ReasonFailureNonRetryable:      true,
			cord.ReasonFailureAttemptsExhausted: true,
			cord.ReasonFailureLeaseExpired:      true,
			cord.ReasonLegacyUnknown:            true,
		},
		cord.NodeStateCanceled: {
			cord.ReasonCanceledByRequest:    true,
			cord.ReasonCanceledByRunFailure: true,
			cord.ReasonLegacyUnknown:        true,
		},
	}

	for _, state := range states {
		for _, reason := range reasons {
			assert.Equal(t, legal[state][reason], state.AllowsReason(reason), "%s/%s", state, reason)
		}
	}
}

func allReasons() []cord.TerminalReason {
	return []cord.TerminalReason{
		"",
		cord.ReasonSucceeded,
		cord.ReasonCanceledByRequest,
		cord.ReasonCanceledByRunFailure,
		cord.ReasonFailureNonRetryable,
		cord.ReasonFailureAttemptsExhausted,
		cord.ReasonFailureLeaseExpired,
		cord.ReasonLegacyUnknown,
		"future_reason",
	}
}
