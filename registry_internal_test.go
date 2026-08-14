package cord

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCord_RegisteredFunctionsCachesUntilRegistrationChanges(t *testing.T) {
	t.Parallel()

	runtime := &Cord{registry: make(map[string]registeredInvocation)}
	invoke := func(context.Context, []storage.EncodedPayload) (storage.EncodedPayload, error) {
		return nil, nil
	}

	require.NoError(t, runtime.register(nodeDefinition{functionKey: "first", signature: "signature-1"}, invoke))
	first, err := runtime.registeredFunctions()
	require.NoError(t, err)
	assert.JSONEq(t, `{"first":"signature-1"}`, string(first))

	cachedFirst, err := runtime.registeredFunctions()
	require.NoError(t, err)
	assert.Same(t, &first[0], &cachedFirst[0])

	require.NoError(t, runtime.register(nodeDefinition{functionKey: "second", signature: "signature-2"}, invoke))
	second, err := runtime.registeredFunctions()
	require.NoError(t, err)
	assert.JSONEq(t, `{"first":"signature-1","second":"signature-2"}`, string(second))
	assert.NotSame(t, &first[0], &second[0])

	require.NoError(t, runtime.register(nodeDefinition{functionKey: "second", signature: "signature-2"}, invoke))
	cachedSecond, err := runtime.registeredFunctions()
	require.NoError(t, err)
	assert.Same(t, &second[0], &cachedSecond[0])
}

func TestCord_RegisteredFunctionsSupportsConcurrentRegistration(t *testing.T) {
	t.Parallel()

	runtime := &Cord{registry: make(map[string]registeredInvocation)}
	invoke := func(context.Context, []storage.EncodedPayload) (storage.EncodedPayload, error) {
		return nil, nil
	}

	const (
		readerCount       = 8
		registrationCount = 100
	)

	start := make(chan struct{})
	errors := make(chan error, readerCount)

	var readers sync.WaitGroup

	readers.Add(readerCount)

	for range readerCount {
		go readRegisteredFunctions(start, errors, &readers, runtime, registrationCount)
	}

	close(start)

	for index := range registrationCount {
		require.NoError(t, runtime.register(nodeDefinition{
			functionKey: fmt.Sprintf("function-%03d", index),
			signature:   fmt.Sprintf("signature-%03d", index),
		}, invoke))
	}

	readers.Wait()
	close(errors)

	for err := range errors {
		require.NoError(t, err)
	}

	snapshot, err := runtime.registeredFunctions()
	require.NoError(t, err)

	var registrations map[string]string
	require.NoError(t, json.Unmarshal(snapshot, &registrations))
	assert.Len(t, registrations, registrationCount)
}

func readRegisteredFunctions(
	start <-chan struct{},
	errors chan<- error,
	readers *sync.WaitGroup,
	runtime *Cord,
	readCount int,
) {
	defer readers.Done()

	<-start

	for range readCount {
		snapshot, err := runtime.registeredFunctions()
		if err != nil {
			errors <- err

			return
		}

		if len(snapshot) == 0 {
			continue
		}

		var registrations map[string]string
		if err := json.Unmarshal(snapshot, &registrations); err != nil {
			errors <- err

			return
		}
	}
}

func BenchmarkCord_RegisteredFunctions(b *testing.B) {
	runtime := &Cord{registry: make(map[string]registeredInvocation)}
	invoke := func(context.Context, []storage.EncodedPayload) (storage.EncodedPayload, error) {
		return nil, nil
	}

	const registrySize = 5_000
	for index := range registrySize {
		err := runtime.register(nodeDefinition{
			functionKey: fmt.Sprintf("function-%04d", index),
			signature:   fmt.Sprintf("signature-%04d", index),
		}, invoke)
		require.NoError(b, err)
	}

	_, err := runtime.registeredFunctions()
	require.NoError(b, err)

	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_, err := runtime.registeredFunctions()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("rebuild", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			registrations := make(map[string]string, registrySize)
			for key, entry := range runtime.registry {
				registrations[key] = entry.signature
			}

			_, err := json.Marshal(registrations)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
