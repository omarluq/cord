package cord

import (
	"context"

	"github.com/omarluq/cord/internal/storage"
)

type activeAttemptKey struct {
	runID      storage.RunID
	nodeID     storage.NodeID
	leaseOwner string
	generation int64
}

type activeAttempt struct {
	cancel context.CancelFunc
}

func newActiveAttemptKey(claim *storage.Claim) activeAttemptKey {
	return activeAttemptKey{
		runID:      claim.RunID,
		nodeID:     claim.NodeID,
		leaseOwner: claim.Lease.Owner,
		generation: claim.Lease.Generation,
	}
}

func (c *Cord) registerActiveAttemptLocked(
	claim *storage.Claim,
	cancel context.CancelFunc,
) (unregister func()) {
	key := newActiveAttemptKey(claim)
	attempt := &activeAttempt{cancel: cancel}

	if c.activeAttempts == nil {
		c.activeAttempts = make(map[storage.RunID]map[activeAttemptKey]*activeAttempt)
	}

	if c.activeAttempts[claim.RunID] == nil {
		c.activeAttempts[claim.RunID] = make(map[activeAttemptKey]*activeAttempt)
	}

	if previous := c.activeAttempts[claim.RunID][key]; previous != nil {
		previous.cancel()
	}

	c.activeAttempts[claim.RunID][key] = attempt

	return func() {
		c.activeMu.Lock()
		defer c.activeMu.Unlock()

		attempts := c.activeAttempts[claim.RunID]
		if attempts[key] != attempt {
			return
		}

		delete(attempts, key)

		if len(attempts) == 0 {
			delete(c.activeAttempts, claim.RunID)
		}
	}
}

func (c *Cord) cancelActiveAttempts(runID storage.RunID) {
	c.activeMu.Lock()
	attempts := c.activeAttempts[runID]
	delete(c.activeAttempts, runID)
	c.activeMu.Unlock()

	for _, attempt := range attempts {
		attempt.cancel()
	}
}
