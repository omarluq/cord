package cord

func (c *Cord) publishCompletion(
	poll *completionPoll,
	observation *completionObservation,
	final bool,
) {
	c.waiterMu.Lock()
	fanout := final || poll.latest == nil
	poll.latest = observation

	waiters := make([]chan completionObservation, 0, len(poll.waiters))
	if fanout {
		for _, waiter := range poll.waiters {
			waiters = append(waiters, waiter.observations)
		}
	}
	c.waiterMu.Unlock()

	for _, waiter := range waiters {
		publishCompletionToWaiter(waiter, observation)
	}
}

func publishCompletionToWaiter(
	waiter chan completionObservation,
	observation *completionObservation,
) {
	select {
	case waiter <- *observation:
		return
	default:
	}

	select {
	case <-waiter:
	default:
	}

	select {
	case waiter <- *observation:
	default:
	}
}
