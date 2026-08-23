package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/samber/hot"
)

type compilationArtifact struct {
	compression *compressedRepresentation
	boundary    string
	wasm        []byte
	graph       protocol.Graph
}

type compressedRepresentation struct {
	err  error
	body []byte
	once sync.Once
}

type compilation struct {
	artifact *compilationArtifact
	err      error
	done     chan struct{}
}

type compilationCache struct {
	ctx      context.Context
	values   *hot.HotCache[[sha256.Size]byte, compilationArtifact]
	inflight map[[sha256.Size]byte]*compilation
	timeout  time.Duration

	mu sync.Mutex
}

func newCompilationCache(ctx context.Context, cfg *config) *compilationCache {
	return &compilationCache{
		ctx: ctx,
		values: hot.NewHotCache[
			[sha256.Size]byte,
			compilationArtifact,
		](hot.LRU, cfg.cacheCapacity).
			WithTTL(cfg.cacheTTL).
			Build(),
		inflight: make(map[[sha256.Size]byte]*compilation),
		timeout:  cfg.compileTimeout,
		mu:       sync.Mutex{},
	}
}

func (cache *compilationCache) load(
	ctx context.Context,
	source string,
	loader func(context.Context) (compilationArtifact, error),
) (compilationArtifact, error) {
	key := sha256.Sum256([]byte(source))

	if artifact, found, err := cache.values.Get(key); err != nil {
		return compilationArtifact{}, fmt.Errorf("load compiled artifact: %w", err)
	} else if found {
		return artifact, nil
	}

	cache.mu.Lock()

	flight, found := cache.inflight[key]
	if !found {
		// Recheck under the flight lock so a completed compilation cannot be
		// missed between the initial cache lookup and registering a new flight.
		if artifact, cached, err := cache.values.Get(key); err != nil {
			cache.mu.Unlock()

			return compilationArtifact{}, fmt.Errorf("load compiled artifact: %w", err)
		} else if cached {
			cache.mu.Unlock()

			return artifact, nil
		}

		flight = &compilation{
			artifact: nil,
			err:      nil,
			done:     make(chan struct{}),
		}
		cache.inflight[key] = flight

		go cache.compile(key, flight, loader)
	}
	cache.mu.Unlock()

	select {
	case <-flight.done:
		if flight.err != nil {
			return compilationArtifact{}, fmt.Errorf("load compiled artifact: %w", flight.err)
		}

		return *flight.artifact, nil
	case <-ctx.Done():
		return compilationArtifact{}, fmt.Errorf("load compiled artifact: %w", ctx.Err())
	}
}

func (cache *compilationCache) compile(
	key [sha256.Size]byte,
	flight *compilation,
	loader func(context.Context) (compilationArtifact, error),
) {
	// Shared work belongs to the service rather than any waiter. In particular,
	// it is intentionally allowed to finish and populate the cache after every
	// waiter has left, bounded by the service lifecycle and compilation timeout.
	ctx, cancel := context.WithTimeout(cache.ctx, cache.timeout)
	defer cancel()

	artifact, err := loader(ctx)
	flight.artifact, flight.err = &artifact, err

	if flight.err == nil {
		// Failed or interrupted loads are never cached.
		cache.values.Set(key, artifact)
	}

	cache.mu.Lock()
	delete(cache.inflight, key)
	close(flight.done)
	cache.mu.Unlock()
}
