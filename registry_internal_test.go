package cord

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	firstFunction   = "first"
	secondFunction  = "second"
	firstSignature  = "signature-1"
	secondSignature = "signature-2"
)

func TestCord_RegisteredFunctionsCachesUntilRegistrationChanges(t *testing.T) {
	t.Parallel()

	runtime := &Cord{registry: make(map[string]registeredInvocation)}
	invoke := func(context.Context, []storage.EncodedPayload) (storage.EncodedPayload, error) {
		return nil, nil
	}

	require.NoError(t, runtime.register(nodeDefinition{functionKey: firstFunction, signature: firstSignature}, invoke))
	first := runtime.registeredFunctions()
	assert.Equal(t, []storage.FunctionRegistration{{Key: firstFunction, Signature: firstSignature}}, first)

	cachedFirst := runtime.registeredFunctions()
	assert.Same(t, &first[0], &cachedFirst[0])

	require.NoError(t, runtime.register(
		nodeDefinition{functionKey: secondFunction, signature: secondSignature}, invoke,
	))
	second := runtime.registeredFunctions()
	assert.Equal(t, []storage.FunctionRegistration{
		{Key: firstFunction, Signature: firstSignature},
		{Key: secondFunction, Signature: secondSignature},
	}, second)
	assert.NotSame(t, &first[0], &second[0])

	require.NoError(t, runtime.register(
		nodeDefinition{functionKey: secondFunction, signature: secondSignature}, invoke,
	))
	cachedSecond := runtime.registeredFunctions()
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
	readErrors := make(chan error, readerCount)

	var readers sync.WaitGroup

	readers.Add(readerCount)

	for range readerCount {
		go readRegisteredFunctions(start, readErrors, &readers, runtime, registrationCount)
	}

	close(start)

	for index := range registrationCount {
		require.NoError(t, runtime.register(nodeDefinition{
			functionKey: fmt.Sprintf("function-%03d", index),
			signature:   fmt.Sprintf("signature-%03d", index),
		}, invoke))
	}

	readers.Wait()
	close(readErrors)

	for err := range readErrors {
		require.NoError(t, err)
	}

	snapshot := runtime.registeredFunctions()

	assert.Len(t, snapshot, registrationCount)
	assert.IsIncreasing(t, registrationKeys(snapshot))
}

func readRegisteredFunctions(
	start <-chan struct{},
	errs chan<- error,
	readers *sync.WaitGroup,
	runtime *Cord,
	readCount int,
) {
	defer readers.Done()

	<-start

	for range readCount {
		snapshot := runtime.registeredFunctions()

		if len(snapshot) == 0 {
			continue
		}

		for _, registration := range snapshot {
			if registration.Key == "" || registration.Signature == "" {
				errs <- errors.New("incomplete registration")

				return
			}
		}
	}
}

func registrationKeys(registrations []storage.FunctionRegistration) []string {
	keys := make([]string, 0, len(registrations))
	for _, registration := range registrations {
		keys = append(keys, registration.Key)
	}

	return keys
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

	_ = runtime.registeredFunctions()

	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_ = runtime.registeredFunctions()
		}
	})

	b.Run("rebuild", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			runtime.mu.Lock()
			runtime.registrations = nil
			runtime.mu.Unlock()

			_ = runtime.registeredFunctions()
		}
	})
}
