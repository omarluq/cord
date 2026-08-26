package cord

import (
	"context"
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func (c *Cord) acquireHeartbeatCall(ctx context.Context) (chan struct{}, bool) {
	c.lifecycleMu.Lock()
	if c.heartbeatCalls == nil {
		capacity := cap(c.slots)
		if capacity == 0 {
			capacity = 1
		}

		c.heartbeatCalls = make(chan struct{}, capacity)
	}

	heartbeatCalls := c.heartbeatCalls
	c.lifecycleMu.Unlock()

	select {
	case heartbeatCalls <- struct{}{}:
		return heartbeatCalls, true
	case <-ctx.Done():
		return nil, false
	default:
		return nil, false
	}
}

func releaseHeartbeatCall(heartbeatCalls chan struct{}) {
	<-heartbeatCalls
}

func (c *Cord) heartbeatOnce(ctx context.Context, claim *storage.Claim) (heartbeatOutcome, time.Duration) {
	accepted, remaining, err := c.store.HeartbeatNode(ctx, claim.RunID, claim.NodeID, claim.Lease, c.leaseTTL)
	if err != nil {
		if ctx.Err() == nil {
			c.reportSchedulerError(fmt.Errorf("cord: heartbeat node: %w", err))
		}

		return heartbeatRetryable, 0
	}

	if !accepted {
		return heartbeatLost, 0
	}

	return heartbeatAccepted, remaining
}
