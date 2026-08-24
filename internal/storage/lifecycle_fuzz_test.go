package storage_test

import (
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

func FuzzPersistedLifecycleVocabulary(f *testing.F) {
	seeds := []string{
		"",
		string(storage.RunRunning), string(storage.RunCanceling), string(storage.RunCompleted),
		string(storage.RunFailed), string(storage.RunCanceled), string(storage.NodePending),
		string(storage.NodeReady), string(storage.NodeRetryWait), string(storage.ReasonSucceeded),
		string(storage.ReasonCanceledByRequest), string(storage.ReasonCanceledByRunFailure),
		string(storage.ReasonFailureNonRetryable), string(storage.ReasonFailureAttemptsExhausted),
		string(storage.ReasonFailureLeaseExpired), string(storage.ReasonLegacyUnknown),
		"future_value", "COMP" + "LETED", "\x00",
	}
	for _, state := range seeds {
		for _, reason := range seeds {
			f.Add(state, reason, int64(1))
		}
	}

	f.Fuzz(func(t *testing.T, stateValue, reasonValue string, versionValue int64) {
		runState := storage.RunStatus(stateValue)
		nodeState := storage.NodeStatus(stateValue)
		reason := storage.TerminalReason(reasonValue)
		version := storage.LifecycleVersion(versionValue)

		assertRunVocabularyProperties(t, runState, reason)
		assertNodeVocabularyProperties(t, nodeState, reason)

		if version.IsKnown() != (version == storage.LifecycleVersion1) {
			t.Fatalf("LifecycleVersion(%d).IsKnown() is inconsistent", versionValue)
		}

		if reason.IsKnown() && reason == "" {
			t.Fatal("empty terminal reason reported as known")
		}
	})
}

func assertRunVocabularyProperties(t *testing.T, state storage.RunStatus, reason storage.TerminalReason) {
	t.Helper()

	terminal, known := state.Terminal()
	assertKnownTerminalAgreement(t, "run", string(state), terminal, known, state.IsKnown())
	assertReasonProperties(t, "run", string(state), string(reason), terminal, known,
		state.AllowsReason(reason), reason.IsKnown())

	if mapped, ok := state.LegacyReason(); ok && (!terminal || !state.AllowsReason(mapped)) {
		t.Fatalf("run state %q has invalid legacy reason %q", state, mapped)
	}
}

func assertNodeVocabularyProperties(t *testing.T, state storage.NodeStatus, reason storage.TerminalReason) {
	t.Helper()

	terminal, known := state.Terminal()
	assertKnownTerminalAgreement(t, "node", string(state), terminal, known, state.IsKnown())
	assertReasonProperties(t, "node", string(state), string(reason), terminal, known,
		state.AllowsReason(reason), reason.IsKnown())

	for _, runState := range []storage.RunStatus{
		storage.RunRunning, storage.RunCanceling, storage.RunCompleted,
		storage.RunFailed, storage.RunCanceled, "future",
	} {
		if mapped, ok := state.LegacyReason(runState); ok && (!terminal || !state.AllowsReason(mapped)) {
			t.Fatalf("node state %q under run %q has invalid legacy reason %q", state, runState, mapped)
		}
	}
}

func assertKnownTerminalAgreement(
	t *testing.T,
	kind, state string,
	terminal, terminalKnown, stateKnown bool,
) {
	t.Helper()

	if terminalKnown != stateKnown {
		t.Fatalf("%s state %q: Terminal known=%t, IsKnown=%t", kind, state, terminalKnown, stateKnown)
	}

	if !terminalKnown && terminal {
		t.Fatalf("unknown %s state %q reported terminal", kind, state)
	}
}

func assertReasonProperties(
	t *testing.T,
	kind, state, reason string,
	terminal, known, allowed, reasonKnown bool,
) {
	t.Helper()

	if !known && allowed {
		t.Fatalf("unknown %s state %q accepted reason %q", kind, state, reason)
	}

	if !allowed {
		return
	}

	if terminal != (reason != "") {
		t.Fatalf("%s state %q accepted inconsistent reason %q", kind, state, reason)
	}

	if reason != "" && !reasonKnown {
		t.Fatalf("%s state %q accepted unknown reason %q", kind, state, reason)
	}
}
