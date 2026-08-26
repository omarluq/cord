package cord

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func TestCord_SchedulerErrorCallbackDoesNotBlockReporting(t *testing.T) {
	t.Parallel()

	callbackStarted := make(chan struct{}, 1)
	callbackRelease := make(chan struct{})

	var callbackCalls atomic.Int32

	runtime := newSchedulerCallbackRuntime(t, func(error) {
		callbackCalls.Add(1)

		select {
		case callbackStarted <- struct{}{}:
		default:
		}

		<-callbackRelease
	})

	runtime.reportSchedulerError(context.Canceled)
	<-callbackStarted

	reported := make(chan struct{})

	go func() {
		runtime.reportSchedulerError(context.DeadlineExceeded)
		close(reported)
	}()

	<-reported

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.Close() }()

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close waited for a blocked scheduler error callback")
	}

	close(callbackRelease)

	select {
	case <-runtime.errorReporterDone:
	case <-time.After(time.Second):
		t.Fatal("released scheduler error reporter did not exit after shutdown")
	}

	require.Equal(t, int32(1), callbackCalls.Load(), "shutdown must discard queued reports")
}

func TestCord_SchedulerErrorReportingRacesWithShutdown(t *testing.T) {
	t.Parallel()

	callbackStarted := make(chan struct{})
	callbackRelease := make(chan struct{})
	runtime := newSchedulerCallbackRuntime(t, func(error) {
		select {
		case <-callbackStarted:
		default:
			close(callbackStarted)
		}

		<-callbackRelease
	})

	runtime.reportSchedulerError(errors.New("block callback"))
	<-callbackStarted

	start := make(chan struct{})

	var reporters sync.WaitGroup
	for reporter := range 32 {
		reporters.Go(func() {
			<-start

			for report := range 100 {
				runtime.reportSchedulerError(fmt.Errorf("reporter %d error %d", reporter, report))
			}
		})
	}

	shutdownDone := make(chan error, 1)

	go func() {
		<-start

		shutdownDone <- runtime.Close()
	}()

	close(start)

	reporters.Wait()

	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("concurrent reporting prevented shutdown")
	}

	close(callbackRelease)

	select {
	case <-runtime.errorReporterDone:
	case <-time.After(time.Second):
		t.Fatal("scheduler error reporter did not exit after concurrent shutdown")
	}
}
