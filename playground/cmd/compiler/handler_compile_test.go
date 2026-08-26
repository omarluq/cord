package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandlerCachesSuccessfulCompilations(t *testing.T) {
	t.Parallel()

	calls := 0
	handler := newHandler(
		testConfigPointer(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			calls++

			return []byte("wasm"), nil
		}),
	)

	for range 2 {
		response, writer := responseRecorder()
		handler.ServeHTTP(
			writer,
			compileRequestForTest(t.Context()),
		)
		require.Equal(t, http.StatusOK, response.Code)
	}

	require.Equal(t, 1, calls)
}

func TestHandlerDeduplicatesConcurrentCompilations(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})

	var calls atomic.Int32

	handler := newHandler(
		testConfigPointer(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			calls.Add(1)
			close(started)
			<-release

			return []byte("wasm"), nil
		}),
	)

	responses := make(chan int, 2)

	for range 2 {
		go func() {
			response, writer := responseRecorder()
			handler.ServeHTTP(writer, compileRequestForTest(t.Context()))

			responses <- response.Code
		}()
	}

	<-started
	close(release)

	require.Equal(t, http.StatusOK, <-responses)
	require.Equal(t, http.StatusOK, <-responses)
	require.Equal(t, int32(1), calls.Load())
}

func TestHandlerLimitsDistinctConcurrentCompilations(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once

	handler := newHandler(testConfigPointer(), compilerFunc(func(context.Context, string) ([]byte, error) {
		once.Do(func() { close(started) })
		<-release

		return []byte("wasm"), nil
	}))

	firstDone := make(chan int)

	go func() {
		response, writer := responseRecorder()
		handler.ServeHTTP(writer, compileRequestForTest(t.Context()))

		firstDone <- response.Code
	}()

	<-started

	response := httptest.NewRecorder()
	handler.ServeHTTP(
		response,
		compileRequestWithSource(t.Context(), "package main\n"),
	)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, "1", response.Header().Get("Retry-After"))
	close(release)
	require.Equal(t, http.StatusOK, <-firstDone)
}

func TestHandlerReturnsCompilerDiagnostics(t *testing.T) {
	t.Parallel()

	handler := newHandler(testConfigPointer(), compilerFunc(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("compile source: undefined: missing")
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, compileRequestForTest(t.Context()))
	require.Equal(t, http.StatusUnprocessableEntity, response.Code)
	require.JSONEq(
		t,
		`{"error":"load compiled artifact: compile workflow: compile source: undefined: missing"}`,
		response.Body.String(),
	)
}

func TestHandlerTimesOutCompilation(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.compileTimeout = time.Millisecond
	handler := newHandler(&cfg, compilerFunc(func(ctx context.Context, _ string) ([]byte, error) {
		<-ctx.Done()

		return nil, ctx.Err()
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, compileRequestForTest(t.Context()))
	require.Equal(t, http.StatusGatewayTimeout, response.Code)
}
