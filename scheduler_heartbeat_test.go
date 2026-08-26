package cord_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type leaseTestInput struct {
	Directory string
}

type workflowResult struct {
	err   error
	value string
}

func probedLeaseStep(ctx context.Context, input leaseTestInput) (string, error) {
	first := filepath.Join(input.Directory, "first")

	err := os.Mkdir(first, 0o700)
	if err == nil {
		return runFirstLeaseInvocation(ctx, input.Directory)
	}

	if !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("create first-invocation marker: %w", err)
	}

	if markerErr := writeMarker(input.Directory, "second-started"); markerErr != nil {
		return "", markerErr
	}

	if waitErr := waitForMarker(ctx, input.Directory, "release-second"); waitErr != nil {
		return "", waitErr
	}

	return "winner", nil
}

func runFirstLeaseInvocation(ctx context.Context, directory string) (string, error) {
	if err := writeMarker(directory, "first-started"); err != nil {
		return "", err
	}

	select {
	case <-ctx.Done():
		if err := writeMarker(directory, "first-canceled"); err != nil {
			return "", err
		}

		// The lease-loss context is already canceled; keep the stale invocation
		// in flight until the test explicitly releases it.
		if err := waitForMarker(context.WithoutCancel(ctx), directory, "release-first"); err != nil {
			return "", err
		}

		if err := writeMarker(directory, "first-finished"); err != nil {
			return "", err
		}

		return "stale", nil
	case <-markerSignal(ctx, directory, "release-first"):
		return "winner", nil
	}
}

func writeMarker(directory, name string) error {
	if err := os.WriteFile(filepath.Join(directory, name), nil, 0o600); err != nil {
		return fmt.Errorf("write lease marker %q: %w", name, err)
	}

	return nil
}

func waitForMarker(ctx context.Context, directory, name string) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for lease marker %q: %w", name, ctx.Err())
	case <-markerSignal(ctx, directory, name):
		return nil
	}
}

func markerSignal(ctx context.Context, directory, name string) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		for {
			if _, err := os.Stat(filepath.Join(directory, name)); err == nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return done
}

func TestScheduler_HeartbeatKeepsLongRunningInvocationLeased(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	options := testOptions()
	first := newRuntimeWithOptions(t, database, options)
	second := newRuntimeWithOptions(t, database, options)
	flow := first.From("test-workflow", probedLeaseStep)
	second.From("test-workflow", probedLeaseStep)

	directory := t.TempDir()
	cleanupLeaseMarkers(t, directory)

	result := runLeaseWorkflow(t, flow, leaseTestInput{Directory: directory})

	waitMarker(t, directory, "first-started")
	initialExpiry := readLeaseExpiry(t, database)
	require.Eventually(t, func() bool {
		return readLeaseExpiry(t, database).After(initialExpiry)
	}, 5*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return readLeaseExpiry(t, database).After(time.Now().Add(options.LeaseTTL / 2))
	}, 5*time.Second, options.HeartbeatInterval)
	assertNodeLeaseState(t, database, "running", 1)
	require.NoError(t, writeMarker(directory, "release-first"))

	completed := waitWorkflowResult(t, result)
	require.NoError(t, completed.err)
	assert.Equal(t, "winner", completed.value)
}
