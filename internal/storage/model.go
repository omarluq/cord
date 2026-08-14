package storage

import "time"

// RunID identifies a persisted run.
type RunID string

// NodeID identifies a node within a persisted run.
type NodeID string

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

// Run is the storage representation of a run.
type Run struct {
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        *time.Time
	ID                 RunID
	WorkflowName       string
	DefinitionHash     string
	TerminalNodeID     NodeID
	Status             RunStatus
	Input              EncodedPayload
	Output             EncodedPayload
	Error              EncodedPayload
	MaxAttempts        int
	RetryBaseDelay     time.Duration
	RetryMaxDelay      time.Duration
	RetryPolicyVersion int
}

// Lease identifies the executor and fencing generation that own a running node.
type Lease struct {
	ExpiresAt  time.Time
	Owner      string
	Generation int64
}

// Node is the storage representation of a node.
type Node struct {
	AvailableAt   time.Time
	CompletedAt   *time.Time
	StartedAt     *time.Time
	SignatureHash string
	RunID         RunID
	ID            NodeID
	FunctionKey   string
	Status        NodeStatus
	Lease         Lease
	Error         EncodedPayload
	Output        EncodedPayload
	RemainingDeps int
	Attempt       int
}

// Edge is a persisted dependency relationship between two nodes in one run.
type Edge struct {
	RunID       RunID
	Parent      NodeID
	Child       NodeID
	ParentOrder int
}

// Transition describes a requested compare-and-swap state change.
type Transition[S ~string] struct {
	Expected        S
	Next            S
	LeaseOwner      string
	LeaseGeneration int64
}
