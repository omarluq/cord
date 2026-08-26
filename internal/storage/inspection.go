package storage

import "time"

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
