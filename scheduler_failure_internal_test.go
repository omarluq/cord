package cord

import (
	"errors"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func TestCord_TerminalFailureReason(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		err        error
		name       string
		wantReason storage.TerminalReason
		attempt    int
	}{
		{
			name: "permanent before final attempt", err: Permanent(errors.New("permanent")), attempt: 1,
			wantReason: storage.ReasonFailureNonRetryable,
		},
		{
			name: "retryable final attempt", err: errors.New("exhausted"), attempt: 3,
			wantReason: storage.ReasonFailureAttemptsExhausted,
		},
		{
			name: "permanent final attempt", err: Permanent(errors.New("permanent and exhausted")), attempt: 3,
			wantReason: storage.ReasonFailureNonRetryable,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: storage.RunResult{
				WorkflowName: "", DefinitionHash: "", TerminalSignatureHash: "",
				Status: storage.RunFailed, Output: nil, Error: nil,
				MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
			}}
			runtime := &Cord{store: backend}
			claim := &storage.Claim{
				RunID: raceRunID, NodeID: raceNodeID, FunctionKey: "", SignatureHash: "",
				Lease: storage.Lease{}, Attempt: testCase.attempt, MaxAttempts: 3,
				RetryBaseDelay: time.Second, RetryMaxDelay: time.Second, RetryPolicyVersion: 0,
			}

			require.Error(t, runtime.handleFailure(t.Context(), claim, testCase.err))
			require.Equal(t, failTransition, backend.transition)
			require.Equal(t, testCase.wantReason, backend.terminalReason)
		})
	}
}
