package main

import (
	"crypto/sha256"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/samber/hot"
)

type compilationArtifact struct {
	graph protocol.Graph
	wasm  []byte
}

type compilationCache struct {
	values *hot.HotCache[[sha256.Size]byte, compilationArtifact]
}

func newCompilationCache(cfg config) *compilationCache {
	return &compilationCache{
		values: hot.NewHotCache[
			[sha256.Size]byte,
			compilationArtifact,
		](hot.LRU, cfg.cacheCapacity).
			WithTTL(cfg.cacheTTL).
			Build(),
	}
}

func (cache *compilationCache) get(
	source string,
) (compilationArtifact, bool) {
	artifact, found, err := cache.values.Get(sha256.Sum256([]byte(source)))
	return artifact, found && err == nil
}

func (cache *compilationCache) put(
	source string,
	artifact compilationArtifact,
) {
	cache.values.Set(sha256.Sum256([]byte(source)), artifact)
}
