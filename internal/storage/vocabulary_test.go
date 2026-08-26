package storage_test

import (
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestPersistedLifecycleVocabulary(t *testing.T) {
	t.Parallel()

	runStatuses := []struct {
		status   storage.RunStatus
		value    string
		terminal bool
	}{
		{status: storage.RunRunning, value: "running", terminal: false},
		{status: storage.RunCanceling, value: "canceling", terminal: false},
		{status: storage.RunCompleted, value: "completed", terminal: true},
		{status: storage.RunFailed, value: "failed", terminal: true},
		{status: storage.RunCanceled, value: "canceled", terminal: true},
	}
	for _, testCase := range runStatuses {
		assert.Equal(t, testCase.value, string(testCase.status))
		assert.True(t, testCase.status.IsKnown())
		terminal, known := testCase.status.Terminal()
		assert.True(t, known)
		assert.Equal(t, testCase.terminal, terminal)
	}

	nodeStatuses := []struct {
		status   storage.NodeStatus
		value    string
		terminal bool
	}{
		{status: storage.NodePending, value: "pending", terminal: false},
		{status: storage.NodeReady, value: "ready", terminal: false},
		{status: storage.NodeRunning, value: "running", terminal: false},
		{status: storage.NodeRetryWait, value: "retry_wait", terminal: false},
		{status: storage.NodeCompleted, value: "completed", terminal: true},
		{status: storage.NodeFailed, value: "failed", terminal: true},
		{status: storage.NodeCanceled, value: "canceled", terminal: true},
	}
	for _, testCase := range nodeStatuses {
		assert.Equal(t, testCase.value, string(testCase.status))
		assert.True(t, testCase.status.IsKnown())
		terminal, known := testCase.status.Terminal()
		assert.True(t, known)
		assert.Equal(t, testCase.terminal, terminal)
	}

	reasons := []struct {
		reason storage.TerminalReason
		value  string
	}{
		{reason: storage.ReasonSucceeded, value: "succeeded"},
		{reason: storage.ReasonCanceledByRequest, value: "canceled_by_request"},
		{reason: storage.ReasonCanceledByRunFailure, value: "canceled_by_run_failure"},
		{reason: storage.ReasonFailureNonRetryable, value: "failure_non_retryable"},
		{reason: storage.ReasonFailureAttemptsExhausted, value: "failure_attempts_exhausted"},
		{reason: storage.ReasonFailureLeaseExpired, value: "failure_lease_expired"},
	}
	for _, testCase := range reasons {
		assert.Equal(t, testCase.value, string(testCase.reason))
		assert.True(t, testCase.reason.IsKnown())
	}
}

func TestStorageLifecycleUnknownValuesFailClosed(t *testing.T) {
	t.Parallel()

	for _, status := range []storage.RunStatus{"", "queued", "COMPLETED"} {
		assert.False(t, status.IsKnown())
		terminal, known := status.Terminal()
		assert.False(t, terminal)
		assert.False(t, known)
		assert.False(t, status.AllowsReason(""))
	}

	for _, status := range []storage.NodeStatus{"", "blocked", "COMPLETED"} {
		assert.False(t, status.IsKnown())
		terminal, known := status.Terminal()
		assert.False(t, terminal)
		assert.False(t, known)
		assert.False(t, status.AllowsReason(""))
	}

	for _, reason := range []storage.TerminalReason{"", "unknown", "SUCCEEDED"} {
		assert.False(t, reason.IsKnown())
	}
}

func TestStorageStateReasonPairs(t *testing.T) {
	t.Parallel()

	reasons := allStorageReasons()

	runLegal := map[storage.RunStatus]map[storage.TerminalReason]bool{
		storage.RunRunning:   {"": true},
		storage.RunCanceling: {"": true},
		storage.RunCompleted: {storage.ReasonSucceeded: true},
		storage.RunFailed: {
			storage.ReasonFailureNonRetryable:      true,
			storage.ReasonFailureAttemptsExhausted: true,
			storage.ReasonFailureLeaseExpired:      true,
		},
		storage.RunCanceled: {storage.ReasonCanceledByRequest: true},
	}
	for status, legal := range runLegal {
		for _, reason := range reasons {
			assert.Equal(t, legal[reason], status.AllowsReason(reason), "%s/%s", status, reason)
		}
	}

	nodeLegal := map[storage.NodeStatus]map[storage.TerminalReason]bool{
		storage.NodePending:   {"": true},
		storage.NodeReady:     {"": true},
		storage.NodeRunning:   {"": true},
		storage.NodeRetryWait: {"": true},
		storage.NodeCompleted: {storage.ReasonSucceeded: true},
		storage.NodeFailed: {
			storage.ReasonFailureNonRetryable:      true,
			storage.ReasonFailureAttemptsExhausted: true,
			storage.ReasonFailureLeaseExpired:      true,
		},
		storage.NodeCanceled: {
			storage.ReasonCanceledByRequest:    true,
			storage.ReasonCanceledByRunFailure: true,
		},
	}
	for status, legal := range nodeLegal {
		for _, reason := range reasons {
			assert.Equal(t, legal[reason], status.AllowsReason(reason), "%s/%s", status, reason)
		}
	}
}

func allStorageReasons() []storage.TerminalReason {
	return []storage.TerminalReason{
		"",
		storage.ReasonSucceeded,
		storage.ReasonCanceledByRequest,
		storage.ReasonCanceledByRunFailure,
		storage.ReasonFailureNonRetryable,
		storage.ReasonFailureAttemptsExhausted,
		storage.ReasonFailureLeaseExpired,
		"future_reason",
	}
}
