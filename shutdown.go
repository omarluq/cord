package cord

import (
	"context"
	"errors"
	"fmt"
)

// Close releases resources owned by the runtime. It waits for executing steps
// to return and never closes a caller-owned database. Scheduler error callbacks
// are not part of this wait and may call Close safely. Steps must observe their
// context. Call Shutdown when the caller needs a bounded wait.
func (c *Cord) Close() error {
	return c.Shutdown(context.Background())
}

// Shutdown requests runtime cancellation and waits until its scheduler and
// executing steps exit or ctx is done. It does not wait for scheduler error
// callbacks, so callbacks may call Shutdown safely and a blocked callback may
// outlive the wait. It does not close the caller-owned database.
func (c *Cord) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}

	if ctx == nil {
		return errors.New("cord: shutdown context is nil")
	}

	c.closeOnce.Do(c.beginShutdown)

	select {
	case <-c.shutdownDone:
		return nil
	default:
	}

	select {
	case <-c.shutdownDone:
		return nil
	case <-ctx.Done():
		select {
		case <-c.shutdownDone:
			return nil
		default:
			return fmt.Errorf("cord: wait for shutdown: %w", ctx.Err())
		}
	}
}

// beginShutdown is the shutdown linearization point for submissions. It closes
// admission before canceling runtime work, so later submissions cannot persist.
func (c *Cord) beginShutdown() {
	c.errorReportingOff.Store(true)

	c.admissionMu.Lock()
	c.acceptingRuns = false
	stopNow := c.admittedRuns == 0
	c.admissionMu.Unlock()

	if stopNow {
		c.stopRuntime()
	}
}

// stopRuntime cancels local execution only after every admitted submission has
// finished its persistence attempt. Durable work itself is never canceled.
func (c *Cord) stopRuntime() {
	c.cancel()
	c.clearCompletionWaiters()
}

// admitRun is the submission linearization point. Admission is never revoked:
// once won, a submission may finish persistence after shutdown begins.
func (c *Cord) admitRun() bool {
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()

	if !c.acceptingRuns {
		return false
	}

	c.admittedRuns++

	return true
}

// finishRunAdmission releases admission after persistence and lets a waiting
// shutdown cancel local execution once all admitted submissions are reported.
// It reports whether shutdown began while the persistence attempt was admitted.
func (c *Cord) finishRunAdmission() bool {
	c.admissionMu.Lock()
	c.admittedRuns--
	shutdownStarted := !c.acceptingRuns
	stopNow := shutdownStarted && c.admittedRuns == 0
	c.admissionMu.Unlock()

	if stopNow {
		c.stopRuntime()
	}

	return shutdownStarted
}

func (c *Cord) addGoroutine() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.activeGoroutines++
}

func (c *Cord) goroutineDone() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.activeGoroutines--
	if c.activeGoroutines == 0 {
		close(c.shutdownDone)
	}
}
