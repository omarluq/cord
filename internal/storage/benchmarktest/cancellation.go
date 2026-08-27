// Package benchmarktest provides shared storage benchmark operations.
package benchmarktest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const (
	// FunctionKey identifies the benchmark's durable progress function.
	FunctionKey = "benchmark.CancellationProgress"
	// Signature identifies the benchmark's durable progress function version.
	Signature = "cancellation-progress-v1"
)

const (
	worker   = "cancellation-progress-worker"
	leaseTTL = time.Minute
)

type backend interface {
	ClaimReadyNodeForFunctions(
		context.Context,
		string,
		time.Duration,
		[]storage.FunctionRegistration,
	) (*storage.Claim, bool, error)
	HeartbeatNode(
		context.Context,
		storage.RunID,
		storage.NodeID,
		storage.Lease,
		time.Duration,
	) (bool, time.Duration, error)
	CompleteNode(
		context.Context,
		storage.RunID,
		storage.NodeID,
		storage.Lease,
		storage.EncodedPayload,
	) (bool, error)
}

// Advance claims, heartbeats, and completes the benchmark's unrelated progress node.
func Advance(tb testing.TB, store backend) error {
	tb.Helper()

	claim, claimed, err := store.ClaimReadyNodeForFunctions(
		tb.Context(),
		worker,
		leaseTTL,
		[]storage.FunctionRegistration{{Key: FunctionKey, Signature: Signature}},
	)
	if err != nil {
		return fmt.Errorf("claim unrelated progress node: %w", err)
	}

	if !claimed || claim == nil {
		return errors.New("claim unrelated progress node: no claim")
	}

	accepted, _, err := store.HeartbeatNode(
		tb.Context(), claim.RunID, claim.NodeID, claim.Lease, leaseTTL,
	)
	if err != nil {
		return fmt.Errorf("heartbeat unrelated progress node: %w", err)
	}

	if !accepted {
		return errors.New("heartbeat unrelated progress node: rejected")
	}

	accepted, err = store.CompleteNode(
		tb.Context(), claim.RunID, claim.NodeID, claim.Lease, storage.EncodedPayload(`"done"`),
	)
	if err != nil {
		return fmt.Errorf("complete unrelated progress node: %w", err)
	}

	if !accepted {
		return errors.New("complete unrelated progress node: rejected")
	}

	return nil
}
