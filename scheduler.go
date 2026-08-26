package cord

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

const (
	defaultLeaseTTL          = 30 * time.Second
	defaultHeartbeatInterval = 10 * time.Second
	defaultPollInterval      = 200 * time.Millisecond
)

func (c *Cord) signalScheduler() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Cord) scheduler() {
	defer c.goroutineDone()

	pollTimer := time.NewTimer(c.pollInterval)
	defer pollTimer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
			c.drainReadyNodes()
		case <-pollTimer.C:
			c.poll()
			pollTimer.Reset(c.pollInterval)
		}
	}
}

func (c *Cord) poll() {
	if err := c.maintain(); err != nil {
		c.reportSchedulerError(fmt.Errorf("cord: scheduler maintenance: %w", err))

		return
	}

	c.drainReadyNodes()
}

func (c *Cord) drainReadyNodes() {
	for c.trySchedule() {
	}
}

func (c *Cord) maintain() error {
	if _, err := c.store.PromoteRetries(c.ctx); err != nil {
		return fmt.Errorf("cord: promote retries: %w", err)
	}

	if _, err := c.store.RecoverExpiredLeases(c.ctx); err != nil {
		return fmt.Errorf("cord: recover expired leases: %w", err)
	}

	return nil
}

func (c *Cord) trySchedule() bool {
	select {
	case c.slots <- struct{}{}:
	default:
		return false
	}

	registeredFunctions := c.registeredFunctions()

	claim, ok, err := c.store.ClaimReadyNodeForFunctions(c.ctx, c.owner, c.leaseTTL, registeredFunctions)
	if err != nil || !ok {
		<-c.slots

		if err != nil && c.ctx.Err() == nil {
			c.reportSchedulerError(fmt.Errorf("cord: scheduler claim: %w", err))
		}

		return false
	}

	c.addGoroutine()
	go c.executeClaim(claim)

	return true
}

func (c *Cord) registeredFunctions() []storage.FunctionRegistration {
	c.mu.RLock()

	if len(c.registry) == 0 || c.registrations != nil {
		registrations := c.registrations
		c.mu.RUnlock()

		return registrations
	}

	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.registry) == 0 || c.registrations != nil {
		return c.registrations
	}

	registrations := make([]storage.FunctionRegistration, 0, len(c.registry))
	for key, entry := range c.registry {
		registrations = append(registrations, storage.FunctionRegistration{Key: key, Signature: entry.signature})
	}

	slices.SortFunc(registrations, func(left, right storage.FunctionRegistration) int {
		return strings.Compare(left.Key, right.Key)
	})

	c.registrations = registrations

	return c.registrations
}
