package cord_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func closeBlockingStep(ctx context.Context, directory string) (string, error) {
	if err := writeMarker(directory, "started"); err != nil {
		return "", err
	}

	<-ctx.Done()

	if err := writeMarker(directory, "canceled"); err != nil {
		return "", err
	}

	if err := waitForMarker(context.WithoutCancel(ctx), directory, "release"); err != nil {
		return "", err
	}

	return directory, fmt.Errorf("close blocking step: %w", ctx.Err())
}

func closeRuntime(runtime *cord.Cord, callers int, concurrent bool) []error {
	errors := make([]error, callers)

	if !concurrent {
		for index := range callers {
			errors[index] = runtime.Close()
		}

		return errors
	}

	var callersDone sync.WaitGroup

	for index := range callers {
		callersDone.Go(func() {
			errors[index] = runtime.Close()
		})
	}

	callersDone.Wait()

	return errors
}

func TestCord_CloseIsSafeToCallRepeatedly(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		callers    int
		concurrent bool
	}{
		"sequential": {callers: 2},
		"concurrent": {callers: 32, concurrent: true},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			runtime, err := cord.New(t.Context(), openSQLite(t))
			require.NoError(t, err)

			for _, closeErr := range closeRuntime(runtime, testCase.callers, testCase.concurrent) {
				assert.NoError(t, closeErr)
			}
		})
	}
}

func TestCord_CloseWaitsForExecutingNodes(t *testing.T) {
	t.Parallel()

	runtime, err := cord.New(t.Context(), openSQLite(t), cord.Options{
		PollInterval:      time.Millisecond,
		LeaseTTL:          time.Second,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, runtime.Close()) })

	directory := t.TempDir()
	t.Cleanup(func() { assert.NoError(t, writeMarker(directory, "release")) })

	flow := runtime.From("close-waits-for-execution", closeBlockingStep)

	runDone := make(chan error, 1)

	go func() {
		_, runErr := flow.Run(t.Context(), directory)
		runDone <- runErr
	}()

	waitMarker(t, directory, "started")

	const callers = 8

	closeResults := make(chan error, callers)

	for range callers {
		go func() {
			closeResults <- runtime.Close()
		}()
	}

	waitMarker(t, directory, "canceled")

	select {
	case closeErr := <-closeResults:
		require.Failf(t, "Close returned before the executing node exited", "error: %v", closeErr)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, writeMarker(directory, "release"))

	for range callers {
		require.NoError(t, <-closeResults)
	}

	require.ErrorContains(t, <-runDone, "runtime closed")
}

func TestCord_ShutdownBoundsNonCooperativeStep(t *testing.T) {
	t.Parallel()

	runtime, err := cord.New(t.Context(), openSQLite(t), cord.Options{
		PollInterval: time.Hour,
	})
	require.NoError(t, err)

	directory := t.TempDir()
	flow := runtime.From("bounded-shutdown", closeBlockingStep)
	runDone := make(chan error, 1)

	go func() {
		_, runErr := flow.Run(t.Context(), directory)
		runDone <- runErr
	}()

	waitMarker(t, directory, "started")

	shutdownCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, runtime.Shutdown(shutdownCtx), context.Canceled)
	waitMarker(t, directory, "canceled")

	require.NoError(t, writeMarker(directory, "release"))
	require.NoError(t, runtime.Close())
	require.ErrorContains(t, <-runDone, "runtime closed")
}

func TestCord_ShutdownRejectsNilContext(t *testing.T) {
	t.Parallel()

	runtime, err := cord.New(t.Context(), openSQLite(t))
	require.NoError(t, err)

	var shutdownContext context.Context
	require.ErrorContains(t, runtime.Shutdown(shutdownContext), "context is nil")
	require.NoError(t, runtime.Close())
}

func TestCord_CloseDoesNotAffectOtherRuntimes(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	first, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	second, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })

	require.NoError(t, first.Close())

	result, err := second.From("surviving-runtime", increment).Run(t.Context(), 1)
	require.NoError(t, err)
	assert.Equal(t, 2, result)
}
