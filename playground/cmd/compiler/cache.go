package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

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

type compilationCache struct {
	values *hot.HotCache[[sha256.Size]byte, compilationArtifact]
}

func newCompilationCache(cfg *config) *compilationCache {
	return &compilationCache{
		values: hot.NewHotCache[
			[sha256.Size]byte,
			compilationArtifact,
		](hot.LRU, cfg.cacheCapacity).
			WithTTL(cfg.cacheTTL).
			Build(),
	}
}

func (cache *compilationCache) load(
	source string,
	loader func() (compilationArtifact, error),
) (compilationArtifact, error) {
	key := sha256.Sum256([]byte(source))

	artifact, found, err := cache.values.GetWithLoaders(
		key,
		func([][sha256.Size]byte) (map[[sha256.Size]byte]compilationArtifact, error) {
			loaded, loadErr := loader()
			if loadErr != nil {
				return nil, loadErr
			}

			return map[[sha256.Size]byte]compilationArtifact{key: loaded}, nil
		},
	)
	if err != nil {
		return compilationArtifact{}, fmt.Errorf("load compiled artifact: %w", err)
	}

	if !found {
		return compilationArtifact{}, errors.New("compiler cache loader returned no artifact")
	}

	return artifact, nil
}
