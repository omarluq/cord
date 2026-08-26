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

// Edge is a persisted dependency relationship between two nodes in one run.
type Edge struct {
	RunID       RunID
	Parent      NodeID
	Child       NodeID
	ParentOrder int
}
