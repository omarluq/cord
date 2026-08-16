// Package storage defines Cord's backend-neutral persistence contracts and models.
package storage

import (
	"context"
	"errors"
	"time"
)

// Backend is the behavioral persistence contract consumed by Cord.
type Backend interface {
	CreateRun(context.Context, *RunPlan) error
	ClaimReadyNodeForFunctions(context.Context, string, time.Duration, []byte) (*Claim, bool, error)
	LoadNodeInputs(context.Context, RunID, NodeID) ([]EncodedPayload, error)
	CompleteNode(context.Context, RunID, NodeID, Lease, EncodedPayload) (bool, error)
	RetryNode(context.Context, RunID, NodeID, Lease, EncodedPayload, time.Duration) (bool, error)
	FailNode(context.Context, RunID, NodeID, Lease, EncodedPayload) (bool, error)
	PromoteRetries(context.Context) (int64, error)
	RecoverExpiredLeases(context.Context) (int64, error)
	HeartbeatNode(context.Context, RunID, NodeID, Lease, time.Duration) (bool, time.Time, error)
	GetRunResult(context.Context, RunID) (RunResult, error)
}

// RunPlan is the complete normalized storage plan for one run.
type RunPlan struct {
	Nodes []Node
	Edges []Edge
	Run   Run
}

// Lease fences ownership of a running node.
type Lease struct {
	ExpiresAt  time.Time
	Owner      string
	Generation int64
}

// Claim describes a node won by a worker.
type Claim struct {
	RunID              RunID
	NodeID             NodeID
	FunctionKey        string
	SignatureHash      string
	Lease              Lease
	Attempt            int
	MaxAttempts        int
	RetryBaseDelay     time.Duration
	RetryMaxDelay      time.Duration
	RetryPolicyVersion int
}

// RunResult is the persisted terminal state returned to callers.
type RunResult struct {
	Status RunStatus
	Output EncodedPayload
	Error  EncodedPayload
}

var (
	// ErrRunNotFound indicates that a requested run does not exist.
	ErrRunNotFound = errors.New("run not found")
	// ErrSchemaOutdated indicates that a backend schema is absent or too old.
	ErrSchemaOutdated = errors.New("schema is absent or outdated")
	// ErrSchemaNewer indicates that a backend schema is newer than this runtime.
	ErrSchemaNewer = errors.New("schema is newer than runtime")
)
