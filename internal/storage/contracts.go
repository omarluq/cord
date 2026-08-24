// Package storage defines Cord's backend-neutral persistence contracts and models.
package storage

import (
	"context"
	"errors"
	"time"
)

// Backend is the behavioral persistence contract consumed by Cord.
type Backend interface {
	// CreateRun atomically persists a validated run plan.
	CreateRun(context.Context, *RunPlan) error
	// CreateOrAttachRun atomically persists a validated run plan or attaches to
	// the retained compatible run selected by its idempotency key. It returns
	// the durable run ID and whether this call created the run.
	CreateOrAttachRun(context.Context, *RunPlan) (RunID, bool, error)
	// CancelRun durably cancels a run and reports the state that decided the request.
	CancelRun(context.Context, RunID) (CancellationOutcome, error)
	// ClaimReadyNodeForFunctions claims one eligible node matching a registered function signature.
	ClaimReadyNodeForFunctions(context.Context, string, time.Duration, []FunctionRegistration) (*Claim, bool, error)
	// LoadNodeInputs loads the ordered inputs for a claimed node.
	LoadNodeInputs(context.Context, RunID, NodeID) ([]EncodedPayload, error)
	// CompleteNode records a successful result when the lease still owns the node.
	CompleteNode(context.Context, RunID, NodeID, Lease, EncodedPayload) (bool, error)
	// RetryNode records a transient failure and schedules another attempt.
	RetryNode(context.Context, RunID, NodeID, Lease, EncodedPayload, time.Duration) (bool, error)
	// FailNode records a terminal node failure when the lease still owns the node.
	FailNode(context.Context, RunID, NodeID, Lease, EncodedPayload) (bool, error)
	// PromoteRetries makes retrying nodes eligible once their delay has elapsed.
	PromoteRetries(context.Context) (int64, error)
	// RecoverExpiredLeases readies retryable abandoned nodes and terminalizes exhausted ones.
	RecoverExpiredLeases(context.Context) (int64, error)
	// HeartbeatNode extends a node lease and returns its database-relative remaining lifetime.
	HeartbeatNode(context.Context, RunID, NodeID, Lease, time.Duration) (bool, time.Duration, error)
	// GetRunResult returns the persisted state and payloads for a run.
	GetRunResult(context.Context, RunID) (RunResult, error)
}

// FunctionRegistration identifies one function signature executable by a runtime.
type FunctionRegistration struct {
	Key       string
	Signature string
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
	// Remaining is the conservative database-relative lease lifetime observed
	// when the lease was claimed. It is for local scheduling only and is not
	// persisted or used as a durable ownership fence.
	Remaining time.Duration
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

// CancellationOutcome describes the durable result of a cancellation request.
type CancellationOutcome string

const (
	// CancellationCanceled indicates that this request durably canceled the run.
	CancellationCanceled CancellationOutcome = "canceled"
	// CancellationAlreadyCanceled indicates that the run was already canceled.
	CancellationAlreadyCanceled CancellationOutcome = "already_canceled"
	// CancellationFinished indicates that the run had already completed or failed.
	CancellationFinished CancellationOutcome = "finished"
	// CancellationNotFound indicates that no run exists with the supplied ID.
	CancellationNotFound CancellationOutcome = "not_found"
)

// RunResult is the persisted run state and type identity returned to callers.
type RunResult struct {
	WorkflowName          string
	DefinitionHash        string
	TerminalSignatureHash string
	Status                RunStatus
	Output                EncodedPayload
	Error                 EncodedPayload
	MaxAttempts           int
	RetryBaseDelay        time.Duration
	RetryMaxDelay         time.Duration
	RetryPolicyVersion    int
}

var (
	// ErrRunNotFound indicates that a requested run does not exist.
	ErrRunNotFound = errors.New("run not found")
	// ErrRunConflict indicates that an idempotency key belongs to an incompatible submission.
	ErrRunConflict = errors.New("run submission conflicts with an existing run")
	// ErrSchemaOutdated indicates that a backend schema is absent or too old.
	ErrSchemaOutdated = errors.New("schema is absent or outdated")
	// ErrSchemaNewer indicates that a backend schema is newer than this runtime.
	ErrSchemaNewer = errors.New("schema is newer than runtime")
)
