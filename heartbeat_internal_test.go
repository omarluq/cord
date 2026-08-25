package cord

import (
	"context"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type blockedHeartbeatBackend struct {
	storage.Backend
	started chan struct{}
	release <-chan struct{}
}

func (backend *blockedHeartbeatBackend) HeartbeatNode(
	context.Context,
	storage.RunID,
	storage.NodeID,
	storage.Lease,
	time.Duration,
) (bool, time.Duration, error) {
	select {
	case backend.started <- struct{}{}:
	default:
	}

	<-backend.release

	return true, time.Second, nil
}

func TestCord_BlockedHeartbeatCannotDelayLeaseSafety(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})

	t.Cleanup(func() { close(release) })

	backend := &blockedHeartbeatBackend{started: make(chan struct{}, 1), release: release}
	runtime := &Cord{
		store:             backend,
		heartbeatInterval: 5 * time.Millisecond,
		leaseTTL:          50 * time.Millisecond,
		heartbeatCalls:    make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan bool, 1)
	claim := testClaim("blocked-heartbeat-run", "node")
	claim.Lease.Remaining = 50 * time.Millisecond

	go runtime.heartbeat(ctx, claim, cancel, done)

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not start")
	}

	select {
	case held := <-done:
		assert.False(t, held)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("lease safety waited for blocked heartbeat I/O")
	}
}

func TestCord_HeartbeatCallsAreGloballyBounded(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})

	t.Cleanup(func() {
		if release != nil {
			close(release)
		}
	})

	backend := &blockedHeartbeatBackend{started: make(chan struct{}, 10), release: release}
	runtime := &Cord{
		store:             backend,
		heartbeatInterval: time.Millisecond,
		leaseTTL:          20 * time.Millisecond,
		heartbeatCalls:    make(chan struct{}, 2),
	}

	for index := range 8 {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan bool, 1)
		claim := testClaim(storage.RunID("run"), storage.NodeID("node"))
		claim.Lease.Generation = int64(index + 1)
		claim.Lease.Remaining = 20 * time.Millisecond

		go runtime.heartbeat(ctx, claim, cancel, done)
	}

	for range 2 {
		select {
		case <-backend.started:
		case <-time.After(time.Second):
			t.Fatal("expected bounded heartbeat call did not start")
		}
	}

	select {
	case <-backend.started:
		t.Fatal("heartbeat calls exceeded the runtime bound")
	case <-time.After(50 * time.Millisecond):
	}

	assert.Len(t, runtime.heartbeatCalls, 2)
	close(release)
	release = nil

	require.Eventually(t, func() bool {
		return len(runtime.heartbeatCalls) == 0
	}, time.Second, time.Millisecond, "heartbeat permits were not returned")
}
