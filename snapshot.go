package cord

import "time"

// NodeStateCounts contains an explicit count for every node state.
type NodeStateCounts struct {
	// Pending is the number of nodes with unsatisfied dependencies.
	Pending int
	// Ready is the number of nodes eligible to be claimed.
	Ready int
	// Running is the number of currently leased nodes.
	Running int
	// RetryWait is the number of nodes waiting for a retry deadline.
	RetryWait int
	// Completed is the number of successfully completed nodes.
	Completed int
	// Failed is the number of terminally failed nodes.
	Failed int
	// Canceled is the number of terminally canceled nodes.
	Canceled int
}

// RunReport is an authoritative current snapshot of one durable run. It omits
// payloads and user error messages. The value may be stale immediately after it
// is returned and does not represent event or attempt history. Timestamps are
// database observations normalized to UTC; precision can vary by provider.
type RunReport struct {
	// SubmittedAt is when the run was durably created.
	SubmittedAt time.Time
	// FirstStartedAt is when any node was first claimed, when known.
	FirstStartedAt *time.Time
	// StateChangedAt is when the current run state was durably entered.
	StateChangedAt time.Time
	// FinishedAt is when the run entered a terminal state, when applicable.
	FinishedAt *time.Time
	// TerminalRunnerID identifies the runner that committed a claimed terminal
	// transition, when applicable. It is diagnostic metadata, not authority.
	TerminalRunnerID *RunnerID
	// ID identifies the durable run.
	ID RunID
	// WorkflowName is the durable workflow identity.
	WorkflowName string
	// State is the run's current durable state.
	State RunState
	// Reason is the stable terminal reason, or empty while nonterminal.
	Reason TerminalReason
	// NodeCounts contains counts for every node state in this observation.
	NodeCounts NodeStateCounts
}

// CurrentLease describes the durable fence currently held for a running node.
// RunnerID alone never authorizes a transition; generation and expiry remain
// part of the storage fencing contract.
type CurrentLease struct {
	// ExpiresAt is the database-time lease deadline, normalized to UTC.
	ExpiresAt time.Time
	// RunnerID identifies the runner holding the current lease.
	RunnerID RunnerID
	// Generation is the current lease fencing generation.
	Generation int64
}

// NodeReport is an authoritative current snapshot of one durable run node. It
// contains no input, output, or user error-message data and is not attempt
// history. A returned value may be stale immediately.
type NodeReport struct {
	// EligibleAt is the durable scheduling time. For ready nodes it is the
	// earliest claim time; for retrying nodes it is the retry deadline.
	EligibleAt time.Time
	// FirstStartedAt is the first successful claim time, when known.
	FirstStartedAt *time.Time
	// LastStartedAt is the latest successful claim time, when known.
	LastStartedAt *time.Time
	// StateChangedAt is when the current node state was entered, when known.
	StateChangedAt *time.Time
	// FinishedAt is when the node entered a terminal state, when applicable.
	FinishedAt *time.Time
	// RunnerID identifies the current or most recent successful claimant, when
	// known. It is diagnostic metadata, not an authorization credential.
	RunnerID *RunnerID
	// CurrentLease is present only while the node is running.
	CurrentLease *CurrentLease
	// RunID identifies the node's durable run.
	RunID RunID
	// NodeID is the stable logical node identifier within the run.
	NodeID NodeID
	// FunctionKey identifies the registered function used by the node.
	FunctionKey string
	// State is the node's current durable state.
	State NodeState
	// Reason is the stable terminal reason, or empty while nonterminal.
	Reason TerminalReason
	// Attempt is the number of successful claims made for this node.
	Attempt int
	// MaxAttempts is the persisted attempt limit for this node.
	MaxAttempts int
}

// NodeQuery selects a bounded page of nodes ordered by stable NodeID. State and
// Reason are optional exact filters. ContinuationToken is opaque and must be
// reused only with the same run and filters. PageSize zero uses a conservative
// default; values above the supported maximum are rejected.
type NodeQuery struct {
	// State optionally filters by an exact known node state.
	State *NodeState
	// Reason optionally filters by an exact known terminal reason.
	Reason *TerminalReason
	// ContinuationToken resumes a previous ListRunNodes call.
	ContinuationToken string
	// PageSize is the maximum number of nodes returned in this page.
	PageSize int
}

// NodePage is one bounded page of node snapshots. Pages are individually
// coherent durable observations, but multiple pages are not one historical
// snapshot and may observe intervening transitions.
type NodePage struct {
	// ContinuationToken is nonempty when another page may be requested.
	ContinuationToken string
	// Nodes contains immutable report values ordered by stable NodeID.
	Nodes []NodeReport
}
