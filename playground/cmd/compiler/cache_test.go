package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestCompilationCacheFirstWaiterCancellationDoesNotCancelSharedWork(t *testing.T) {
	t.Parallel()

	cache := newCompilationCache(testConfigPointer())
	started := make(chan struct{})
	release := make(chan struct{})
	loaderContext := make(chan context.Context, 1)

	var calls atomic.Int32

	loader := func(ctx context.Context) (compilationArtifact, error) {
		if err := context.Cause(ctx); err != nil {
			return compilationArtifact{}, fmt.Errorf("loader canceled: %w", err)
		}

		calls.Add(1)

		loaderContext <- ctx

		close(started)
		<-release

		return compilationArtifact{
			compression: nil, identity: nil, boundary: "", wasm: []byte("wasm"), graph: protocol.Graph{},
		}, nil
	}

	firstContext, cancelFirst := context.WithCancel(t.Context())
	first := make(chan error, 1)

	go func() {
		_, err := cache.load(t.Context(), firstContext, "source", loader)
		first <- err
	}()

	<-started

	second := make(chan error, 1)

	go func() {
		_, err := cache.load(t.Context(), t.Context(), "source", loader)
		second <- err
	}()

	cancelFirst()
	require.ErrorIs(t, <-first, context.Canceled)
	require.NoError(t, context.Cause(<-loaderContext))

	close(release)
	require.NoError(t, <-second)
	require.Equal(t, int32(1), calls.Load())
}

func TestCompilationCacheFinishesAfterAllWaitersCancel(t *testing.T) {
	t.Parallel()

	cache := newCompilationCache(testConfigPointer())
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})

	var calls atomic.Int32

	loader := func(ctx context.Context) (compilationArtifact, error) {
		if err := context.Cause(ctx); err != nil {
			return compilationArtifact{}, fmt.Errorf("loader canceled: %w", err)
		}

		calls.Add(1)
		close(started)
		<-release
		close(finished)

		return compilationArtifact{
			compression: nil, identity: nil, boundary: "", wasm: []byte("wasm"), graph: protocol.Graph{},
		}, nil
	}

	firstContext, cancelFirst := context.WithCancel(t.Context())
	secondContext, cancelSecond := context.WithCancel(t.Context())
	waiters := make(chan error, 2)

	go func() {
		_, err := cache.load(t.Context(), firstContext, "source", loader)
		waiters <- err
	}()

	<-started

	go func() {
		_, err := cache.load(t.Context(), secondContext, "source", loader)
		waiters <- err
	}()

	cancelFirst()
	cancelSecond()
	require.ErrorIs(t, <-waiters, context.Canceled)
	require.ErrorIs(t, <-waiters, context.Canceled)

	close(release)
	<-finished

	artifact, err := cache.load(t.Context(), t.Context(), "source", func(context.Context) (compilationArtifact, error) {
		calls.Add(1)

		return compilationArtifact{}, errors.New("cached artifact was not used")
	})
	require.NoError(t, err)
	require.Equal(t, []byte("wasm"), artifact.wasm)
	require.Equal(t, int32(1), calls.Load())
}

func TestCompilationCacheServiceTimeout(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.compileTimeout = 10 * time.Millisecond
	cache := newCompilationCache(&cfg)

	_, err := cache.load(
		t.Context(),
		t.Context(),
		"source",
		func(ctx context.Context) (compilationArtifact, error) {
			<-ctx.Done()

			return compilationArtifact{}, ctx.Err()
		},
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCompilationCacheServiceCancellation(t *testing.T) {
	t.Parallel()

	serviceContext, cancelService := context.WithCancel(t.Context())
	cache := newCompilationCache(testConfigPointer())
	started := make(chan struct{})

	result := make(chan error, 1)

	go func() {
		_, err := cache.load(
			serviceContext,
			t.Context(),
			"source",
			func(ctx context.Context) (compilationArtifact, error) {
				close(started)
				<-ctx.Done()

				return compilationArtifact{}, ctx.Err()
			},
		)
		result <- err
	}()

	<-started
	cancelService()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestCompilationCacheDoesNotCacheLoaderFailure(t *testing.T) {
	t.Parallel()

	cache := newCompilationCache(testConfigPointer())

	var calls atomic.Int32

	loader := func(context.Context) (compilationArtifact, error) {
		if calls.Add(1) == 1 {
			return compilationArtifact{}, errors.New("loader failed")
		}

		return compilationArtifact{
			compression: nil, identity: nil, boundary: "", wasm: []byte("wasm"), graph: protocol.Graph{},
		}, nil
	}

	_, err := cache.load(t.Context(), t.Context(), "source", loader)
	require.ErrorContains(t, err, "loader failed")

	artifact, err := cache.load(t.Context(), t.Context(), "source", loader)
	require.NoError(t, err)
	require.Equal(t, []byte("wasm"), artifact.wasm)
	require.Equal(t, int32(2), calls.Load())
}
