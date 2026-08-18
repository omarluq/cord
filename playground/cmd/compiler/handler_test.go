package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type compilerFunc func(context.Context, string) ([]byte, error)

func (function compilerFunc) Compile(ctx context.Context, source string) ([]byte, error) {
	return function(ctx, source)
}

func testConfig() config {
	return config{
		address: "", allowedOrigin: "https://play.example", cordDirectory: "",
		maxRequestBytes: 128,
		maxSourceBytes:  32,
		compileTimeout:  time.Second,
		maxConcurrency:  1,
		cacheCapacity:   8,
		cacheTTL:        time.Minute,
	}
}

const compileCase = "compile"

func TestHandlerRoutesAndHeaders(t *testing.T) {
	t.Parallel()

	handler := newHandler(testConfig(), compilerFunc(func(_ context.Context, source string) ([]byte, error) {
		require.Equal(t, "package main", source)
		return []byte("wasm"), nil
	}))

	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		contentType string
		status      int
		response    string
	}{
		{name: "health", method: http.MethodGet, path: "/healthz", body: "", contentType: "", status: http.StatusOK, response: "ok\n"},
		{name: "preflight", method: http.MethodOptions, path: compilePath, body: "", contentType: "", status: http.StatusNoContent, response: ""},
		{name: compileCase, method: http.MethodPost, path: compilePath, body: `{"source":"package main"}`, contentType: jsonMediaType, status: http.StatusOK, response: ""},
		{name: "method", method: http.MethodGet, path: compilePath, body: "", contentType: "", status: http.StatusMethodNotAllowed, response: "method not allowed\n"},
		{name: "missing", method: http.MethodGet, path: "/missing", body: "", contentType: "", status: http.StatusNotFound, response: "404 page not found\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequestWithContext(t.Context(), test.method, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", test.contentType)
			request.Header.Set("Origin", "https://play.example")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			require.Equal(t, test.status, response.Code)
			if test.name != compileCase {
				require.Equal(t, test.response, response.Body.String())
			}
			require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
			require.Equal(t, "https://play.example", response.Header().Get("Access-Control-Allow-Origin"))
			if test.name == compileCase {
				require.Contains(t, response.Header().Get("Content-Type"), "multipart/form-data")
				require.Contains(t, response.Body.String(), "wasm")
				require.Contains(t, response.Body.String(), `"nodes":[]`)
			}
		})
	}
}

func TestHandlerRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	handler := newHandler(testConfig(), compilerFunc(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("unexpected compilation")
	}))
	tests := []struct {
		name        string
		body        string
		contentType string
		status      int
	}{
		{name: "media type", body: `{}`, contentType: "text/plain", status: http.StatusUnsupportedMediaType},
		{name: "malformed", body: `{`, contentType: jsonMediaType, status: http.StatusBadRequest},
		{name: "unknown field", body: `{"source":"x","other":1}`, contentType: jsonMediaType, status: http.StatusBadRequest},
		{name: "empty", body: `{"source":""}`, contentType: jsonMediaType, status: http.StatusBadRequest},
		{name: "source limit", body: `{"source":"abcdefghijklmnopqrstuvwxyz0123456789"}`, contentType: jsonMediaType, status: http.StatusRequestEntityTooLarge},
		{name: "request limit", body: `{"source":"` + strings.Repeat("x", 200) + `"}`, contentType: jsonMediaType, status: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, compilePath, strings.NewReader(test.body))
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, test.status, response.Code)
		})
	}
}

func TestHandlerCachesSuccessfulCompilations(t *testing.T) {
	t.Parallel()

	calls := 0
	handler := newHandler(
		testConfig(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			calls++
			return []byte("wasm"), nil
		}),
	)

	for range 2 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(
			response,
			compileRequestForTest(t.Context()),
		)
		require.Equal(t, http.StatusOK, response.Code)
	}
	require.Equal(t, 1, calls)
}

func TestHandlerLimitsConcurrency(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	handler := newHandler(testConfig(), compilerFunc(func(context.Context, string) ([]byte, error) {
		once.Do(func() { close(started) })
		<-release
		return []byte("wasm"), nil
	}))

	firstDone := make(chan int)
	go func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, compileRequestForTest(t.Context()))
		firstDone <- response.Code
	}()
	<-started

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, compileRequestForTest(t.Context()))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, "1", response.Header().Get("Retry-After"))
	close(release)
	require.Equal(t, http.StatusOK, <-firstDone)
}

func TestHandlerReturnsCompilerDiagnostics(t *testing.T) {
	t.Parallel()

	handler := newHandler(testConfig(), compilerFunc(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("compile source: undefined: missing")
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, compileRequestForTest(t.Context()))
	require.Equal(t, http.StatusUnprocessableEntity, response.Code)
	require.JSONEq(t, `{"error":"compile source: undefined: missing"}`, response.Body.String())
}

func TestHandlerTimesOutCompilation(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.compileTimeout = time.Millisecond
	handler := newHandler(cfg, compilerFunc(func(ctx context.Context, _ string) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, compileRequestForTest(t.Context()))
	require.Equal(t, http.StatusGatewayTimeout, response.Code)
}

func compileRequestForTest(ctx context.Context) *http.Request {
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, compilePath, strings.NewReader(`{"source":"package main"}`))
	request.Header.Set("Content-Type", jsonMediaType)
	request.Header.Set("Origin", "https://play.example")
	return request
}
