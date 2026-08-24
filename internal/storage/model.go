package storage

import "time"

// RunID identifies a persisted run.
type RunID string

// NodeID identifies a node within a persisted run.
type NodeID string

// RunnerID identifies one runtime incarnation in persisted diagnostic metadata.
type RunnerID string

// EncodedPayload is an opaque serialized value. A nil payload represents a nullable value.
type EncodedPayload []byte

// RunStatus is the persisted state of a run.
type RunStatus string

const (
	// RunRunning indicates that a run may still make progress.
	RunRunning RunStatus = "running"
	// RunCompleted indicates that a run completed successfully.
	RunCompleted RunStatus = "completed"
	// RunFailed indicates that a run failed terminally.
	RunFailed RunStatus = "failed"
	// RunCanceling indicates that cancellation is being propagated through a run.
	RunCanceling RunStatus = "canceling"
	// RunCanceled indicates that a run was canceled terminally.
	RunCanceled RunStatus = "canceled"
)

// IsKnown reports whether status is a supported persisted run status.
func (status RunStatus) IsKnown() bool {
	switch status {
	case RunRunning, RunCompleted, RunFailed, RunCanceling, RunCanceled:
		return true
	default:
		return false
	}
}

// Terminal reports whether status is terminal and whether status is known.
func (status RunStatus) Terminal() (terminal, known bool) {
	switch status {
	case RunRunning, RunCanceling:
		return false, true
	case RunCompleted, RunFailed, RunCanceled:
		return true, true
	default:
		return false, false
	}
}

// AllowsReason reports whether reason is legal for this run status.
func (status RunStatus) AllowsReason(reason TerminalReason) bool {
	switch status {
	case RunRunning, RunCanceling:
		return reason == ""
	case RunCompleted:
		return reason == ReasonSucceeded
	case RunFailed:
		return reason.isFailure()
	case RunCanceled:
		return reason == ReasonCanceledByRequest
	default:
		return false
	}
}

// NodeStatus is the persisted state of a node.
type NodeStatus string

const (
	// NodePending indicates that a node has unsatisfied dependencies.
	NodePending NodeStatus = "pending"
	// NodeReady indicates that a node is eligible to be claimed.
	NodeReady NodeStatus = "ready"
	// NodeRunning indicates that a node is leased by an executor.
	NodeRunning NodeStatus = "running"
	// NodeRetryWait indicates that a node is waiting for its retry deadline.
	NodeRetryWait NodeStatus = "retry_wait"
	// NodeCompleted indicates that a node completed successfully.
	NodeCompleted NodeStatus = "completed"
	// NodeFailed indicates that a node failed terminally.
	NodeFailed NodeStatus = "failed"
	// NodeCanceled indicates that a node was canceled terminally.
	NodeCanceled NodeStatus = "canceled"
)

// IsKnown reports whether status is a supported persisted node status.
func (status NodeStatus) IsKnown() bool {
	switch status {
	case NodePending, NodeReady, NodeRunning, NodeRetryWait, NodeCompleted, NodeFailed, NodeCanceled:
		return true
	default:
		return false
	}
}

// Terminal reports whether status is terminal and whether status is known.
func (status NodeStatus) Terminal() (terminal, known bool) {
	switch status {
	case NodePending, NodeReady, NodeRunning, NodeRetryWait:
		return false, true
	case NodeCompleted, NodeFailed, NodeCanceled:
		return true, true
	default:
		return false, false
	}
}

// AllowsReason reports whether reason is legal for this node status.
func (status NodeStatus) AllowsReason(reason TerminalReason) bool {
	switch status {
	case NodePending, NodeReady, NodeRunning, NodeRetryWait:
		return reason == ""
	case NodeCompleted:
		return reason == ReasonSucceeded
	case NodeFailed:
		return reason.isFailure()
	case NodeCanceled:
		return reason.isCancellation()
	default:
		return false
	}
}

// TerminalReason is the persisted reason for a terminal lifecycle state.
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
)

// IsKnown reports whether reason is a supported nonempty terminal reason.
func (reason TerminalReason) IsKnown() bool {
	switch reason {
	case ReasonSucceeded, ReasonCanceledByRequest, ReasonCanceledByRunFailure,
		ReasonFailureNonRetryable, ReasonFailureAttemptsExhausted,
		ReasonFailureLeaseExpired:
		return true
	default:
		return false
	}
}

func (reason TerminalReason) isFailure() bool {
	return reason == ReasonFailureNonRetryable ||
		reason == ReasonFailureAttemptsExhausted ||
		reason == ReasonFailureLeaseExpired
}

func (reason TerminalReason) isCancellation() bool {
	return reason == ReasonCanceledByRequest ||
		reason == ReasonCanceledByRunFailure
}

// Run is the storage representation of a run.
type Run struct {
	CreatedAt             time.Time
	UpdatedAt             time.Time
	CompletedAt           *time.Time
	StartedAt             *time.Time
	TerminalReason        *TerminalReason
	TerminalRunnerID      *RunnerID
	ID                    RunID
	WorkflowName          string
	DefinitionHash        string
	IdempotencyKey        *string
	SubmissionFingerprint *string
	TerminalNodeID        NodeID
	Status                RunStatus
	Input                 EncodedPayload
	Output                EncodedPayload
	Error                 EncodedPayload
	MaxAttempts           int
	RetryBaseDelay        time.Duration
	RetryMaxDelay         time.Duration
	RetryPolicyVersion    int
}

// Node is the storage representation of a node.
type Node struct {
	AvailableAt    time.Time
	CompletedAt    *time.Time
	StartedAt      *time.Time
	StateChangedAt *time.Time
	LastStartedAt  *time.Time
	LastRunnerID   *RunnerID
	TerminalReason *TerminalReason
	FunctionKey    string
	RunID          RunID
	ID             NodeID
	SignatureHash  string
	Status         NodeStatus
	Error          EncodedPayload
	Output         EncodedPayload
	Lease          Lease
	RemainingDeps  int
	Attempt        int
}

// NodeStateCounts contains an explicit count for every node state.
type NodeStateCounts struct {
	Pending   int
	Ready     int
	Running   int
	RetryWait int
	Completed int
	Failed    int
	Canceled  int
}

// RunReport is a backend-neutral snapshot of one persisted run.
type RunReport struct {
	SubmittedAt      time.Time
	FirstStartedAt   *time.Time
	StateChangedAt   time.Time
	FinishedAt       *time.Time
	TerminalRunnerID *RunnerID
	ID               RunID
	WorkflowName     string
	State            RunStatus
	Reason           TerminalReason
	NodeCounts       NodeStateCounts
}

// CurrentLease reports the current durable fence for a running node.
type CurrentLease struct {
	ExpiresAt  time.Time
	RunnerID   RunnerID
	Generation int64
}

// NodeReport is a backend-neutral snapshot of one persisted node.
type NodeReport struct {
	EligibleAt     time.Time
	FirstStartedAt *time.Time
	LastStartedAt  *time.Time
	StateChangedAt *time.Time
	FinishedAt     *time.Time
	RunnerID       *RunnerID
	CurrentLease   *CurrentLease
	RunID          RunID
	NodeID         NodeID
	FunctionKey    string
	State          NodeStatus
	Reason         TerminalReason
	Attempt        int
	MaxAttempts    int
}

const (
	// DefaultNodePageSize is used when NodeQuery.PageSize is zero.
	DefaultNodePageSize = 100
	// MaxNodePageSize is the largest node page accepted by storage adapters.
	MaxNodePageSize = 1000
)

// NodeQuery selects an ordered, bounded page of node reports.
type NodeQuery struct {
	State             *NodeStatus
	Reason            *TerminalReason
	ContinuationToken string
	PageSize          int
}

// NodePage is one immutable page of node reports.
type NodePage struct {
	ContinuationToken string
	Nodes             []NodeReport
}

// Edge is a persisted dependency relationship between two nodes in one run.
type Edge struct {
	RunID       RunID
	Parent      NodeID
	Child       NodeID
	ParentOrder int
}
