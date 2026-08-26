package cord

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func TestCord_SchedulerErrorCallbackPanicIsRecovered(t *testing.T) {
	t.Parallel()

	callbackContinued := make(chan struct{})

	var calls atomic.Int32

	runtime := newSchedulerCallbackRuntime(t, func(error) {
		if calls.Add(1) == 1 {
			panic("callback panic")
		}

		close(callbackContinued)
	})

	runtime.reportSchedulerError(errors.New("panic"))
	runtime.reportSchedulerError(errors.New("continue"))

	select {
	case <-callbackContinued:
	case <-time.After(time.Second):
		t.Fatal("callback reporter did not continue after panic")
	}
}

func TestCord_SchedulerErrorCallbackMayCallLifecycleMethods(t *testing.T) {
	t.Parallel()

	testCases := map[string]func(*Cord) error{
		"Close": func(runtime *Cord) error { return runtime.Close() },
		"Shutdown": func(runtime *Cord) error {
			return runtime.Shutdown(context.Background())
		},
	}

	for name, lifecycle := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			result := make(chan error, 1)

			var runtime *Cord

			runtime = newSchedulerCallbackRuntime(t, func(error) {
				result <- lifecycle(runtime)
			})

			runtime.reportSchedulerError(errors.New("lifecycle callback"))

			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("lifecycle call deadlocked in scheduler error callback")
			}
		})
	}
}

func TestCord_SchedulerErrorBurstUsesBoundedOverflowSummary(t *testing.T) {
	t.Parallel()

	callbackStarted := make(chan struct{})
	callbackRelease := make(chan struct{})
	reports := make(chan error, schedulerErrorQueueCapacity+2)
	runtime := newSchedulerCallbackRuntime(t, func(err error) {
		select {
		case <-callbackStarted:
		default:
			close(callbackStarted)
			<-callbackRelease
		}

		reports <- err
	})

	first := errors.New("first")
	runtime.reportSchedulerError(first)
	<-callbackStarted

	const overflow = 7
	for index := range schedulerErrorQueueCapacity + overflow {
		runtime.reportSchedulerError(fmt.Errorf("burst error %d", index))
	}

	close(callbackRelease)

	require.ErrorIs(t, <-reports, first)
	summary := <-reports

	var dropped schedulerErrorsDroppedError
	require.ErrorAs(t, summary, &dropped)
	require.Equal(t, uint64(overflow), dropped.count)

	for range schedulerErrorQueueCapacity {
		select {
		case report := <-reports:
			require.NotErrorAs(t, report, &dropped)
		case <-time.After(time.Second):
			t.Fatal("queued scheduler error was not reported")
		}
	}
}
