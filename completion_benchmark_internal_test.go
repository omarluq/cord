package cord

import (
	"fmt"
	"testing"

	"github.com/omarluq/cord/internal/storage"
)

// BenchmarkCompletionNotification measures completion fanout at representative waiter counts.
func BenchmarkCompletionNotification(b *testing.B) {
	for _, waiterCount := range []int{1, 100, 10_000} {
		b.Run(fmt.Sprintf("waiters=%d", waiterCount), func(b *testing.B) {
			poll := &completionPoll{waiters: make(map[uint64]completionWaiter)}
			waiters := make([]<-chan completionObservation, 0, waiterCount)
			runtime := &Cord{completionWaiters: make(map[storage.RunID]*completionPoll)}

			for index := range waiterCount {
				waiter := make(chan completionObservation, 1)
				poll.waiters[uint64(index)] = completionWaiter{observations: waiter}
				waiters = append(waiters, waiter)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				runtime.publishCompletion(poll, &completionObservation{}, true)

				for _, waiter := range waiters {
					<-waiter
				}
			}
		})
	}
}
