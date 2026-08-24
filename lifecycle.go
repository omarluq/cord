package cord

// RunnerID identifies one Cord runtime incarnation. It is opaque diagnostic
// metadata, not an authorization credential or a lease fencing token.
type RunnerID string

// NodeID identifies a node within a durable workflow run.
type NodeID string

// RunState is the durable current state of a workflow run.
type RunState string

const (
	// RunStateRunning indicates that a run may still make progress.
	RunStateRunning RunState = "running"
	// RunStateCanceling indicates that cancellation is being durably applied.
	RunStateCanceling RunState = "canceling"
	// RunStateCompleted indicates that a run completed successfully.
	RunStateCompleted RunState = "completed"
	// RunStateFailed indicates that a run failed terminally.
	RunStateFailed RunState = "failed"
	// RunStateCanceled indicates that a run was canceled by request.
	RunStateCanceled RunState = "canceled"
)

// IsKnown reports whether state is part of the stable lifecycle vocabulary.
func (state RunState) IsKnown() bool {
	switch state {
	case RunStateRunning, RunStateCanceling, RunStateCompleted, RunStateFailed, RunStateCanceled:
		return true
	default:
		return false
	}
}

// Terminal reports whether state is terminal and whether state is known. A
// caller must check known before interpreting the terminal result.
func (state RunState) Terminal() (terminal, known bool) {
	switch state {
	case RunStateRunning, RunStateCanceling:
		return false, true
	case RunStateCompleted, RunStateFailed, RunStateCanceled:
		return true, true
	default:
		return false, false
	}
}

// AllowsReason reports whether reason is legal for state. It rejects unknown
// states and reasons, missing terminal reasons, and reasons on active states.
func (state RunState) AllowsReason(reason TerminalReason) bool {
	switch state {
	case RunStateRunning, RunStateCanceling:
		return reason == ""
	case RunStateCompleted:
		return reason == ReasonSucceeded
	case RunStateFailed:
		return reason == ReasonFailureNonRetryable ||
			reason == ReasonFailureAttemptsExhausted ||
			reason == ReasonFailureLeaseExpired ||
			reason == ReasonLegacyUnknown
	case RunStateCanceled:
		return reason == ReasonCanceledByRequest
	default:
		return false
	}
}

// NodeState is the durable current state of a workflow node.
type NodeState string

const (
	// NodeStatePending indicates that a node has unsatisfied dependencies.
	NodeStatePending NodeState = "pending"
	// NodeStateReady indicates that a node is eligible to be claimed.
	NodeStateReady NodeState = "ready"
	// NodeStateRunning indicates that a node is leased by a runner.
	NodeStateRunning NodeState = "running"
	// NodeStateRetryWait indicates that a node is waiting for its retry deadline.
	NodeStateRetryWait NodeState = "retry_wait"
	// NodeStateCompleted indicates that a node completed successfully.
	NodeStateCompleted NodeState = "completed"
	// NodeStateFailed indicates that a node failed terminally.
	NodeStateFailed NodeState = "failed"
	// NodeStateCanceled indicates that a node was canceled terminally.
	NodeStateCanceled NodeState = "canceled"
)

// IsKnown reports whether state is part of the stable lifecycle vocabulary.
func (state NodeState) IsKnown() bool {
	switch state {
	case NodeStatePending, NodeStateReady, NodeStateRunning, NodeStateRetryWait,
		NodeStateCompleted, NodeStateFailed, NodeStateCanceled:
		return true
	default:
		return false
	}
}

// Terminal reports whether state is terminal and whether state is known. A
// caller must check known before interpreting the terminal result.
func (state NodeState) Terminal() (terminal, known bool) {
	switch state {
	case NodeStatePending, NodeStateReady, NodeStateRunning, NodeStateRetryWait:
		return false, true
	case NodeStateCompleted, NodeStateFailed, NodeStateCanceled:
		return true, true
	default:
		return false, false
	}
}

// AllowsReason reports whether reason is legal for state. It rejects unknown
// states and reasons, missing terminal reasons, and reasons on active states.
func (state NodeState) AllowsReason(reason TerminalReason) bool {
	switch state {
	case NodeStatePending, NodeStateReady, NodeStateRunning, NodeStateRetryWait:
		return reason == ""
	case NodeStateCompleted:
		return reason == ReasonSucceeded
	case NodeStateFailed:
		return isFailureReason(reason)
	case NodeStateCanceled:
		return reason == ReasonCanceledByRequest ||
			reason == ReasonCanceledByRunFailure ||
			reason == ReasonLegacyUnknown
	default:
		return false
	}
}

func isFailureReason(reason TerminalReason) bool {
	return reason == ReasonFailureNonRetryable ||
		reason == ReasonFailureAttemptsExhausted ||
		reason == ReasonFailureLeaseExpired ||
		reason == ReasonLegacyUnknown
}

// TerminalReason describes why a terminal lifecycle transition was selected.
// The empty value means that a nonterminal resource has no terminal reason.
type TerminalReason string

const (
	// ReasonSucceeded indicates successful completion.
	ReasonSucceeded TerminalReason = "succeeded"
	// ReasonCanceledByRequest indicates that explicit run cancellation won.
	ReasonCanceledByRequest TerminalReason = "canceled_by_request"
	// ReasonCanceledByRunFailure indicates that another node failed the run.
	ReasonCanceledByRunFailure TerminalReason = "canceled_by_run_failure"
	// ReasonFailureNonRetryable indicates a permanent or non-retryable failure.
	ReasonFailureNonRetryable TerminalReason = "failure_non_retryable"
	// ReasonFailureAttemptsExhausted indicates that execution attempts were exhausted.
	ReasonFailureAttemptsExhausted TerminalReason = "failure_attempts_exhausted"
	// ReasonFailureLeaseExpired indicates that the final claim's lease expired.
	ReasonFailureLeaseExpired TerminalReason = "failure_lease_expired"
	// ReasonLegacyUnknown indicates that a legacy terminal cause cannot be known safely.
	ReasonLegacyUnknown TerminalReason = "legacy_unknown"
)

// IsKnown reports whether reason is a nonempty member of the stable lifecycle
// vocabulary.
func (reason TerminalReason) IsKnown() bool {
	switch reason {
	case ReasonSucceeded, ReasonCanceledByRequest, ReasonCanceledByRunFailure,
		ReasonFailureNonRetryable, ReasonFailureAttemptsExhausted,
		ReasonFailureLeaseExpired, ReasonLegacyUnknown:
		return true
	default:
		return false
	}
}
