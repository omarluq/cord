package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/require"
)

type compilerFunc func(context.Context, string) ([]byte, error)

// Compile invokes the test compiler function.
func (function compilerFunc) Compile(ctx context.Context, source string) ([]byte, error) {
	return function(ctx, source)
}

func testConfig() config {
	return config{
		address:         "",
		allowedOrigin:   "https://play.example",
		cordDirectory:   "",
		maxRequestBytes: 128,
		maxSourceBytes:  32,
		maxWASMBytes:    defaultMaxWASMBytes,
		maxDiagnostics:  defaultMaxDiagnostics,
		compileTimeout:  time.Second,
		maxConcurrency:  1,
		cacheCapacity:   8,
		cacheTTL:        time.Minute,
	}
}

func TestWriteArtifactSetsContentLength(t *testing.T) {
	t.Parallel()

	response := httptest.NewRecorder()
	require.NoError(t, writeArtifact(response, protocol.Graph{}, []byte("wasm")))
	require.Equal(t, strconv.Itoa(response.Body.Len()), response.Header().Get("Content-Length"))
	require.Contains(t, response.Body.String(), "wasm")
}

func testConfigPointer() *config {
	cfg := testConfig()

	return &cfg
}

const compileCase = "compile"

func TestHandlerRoutesAndHeaders(t *testing.T) {
	t.Parallel()

	handler := newHandler(testConfigPointer(), compilerFunc(func(_ context.Context, source string) ([]byte, error) {
		require.Equal(t, "package main", source)

		return []byte("wasm"), nil
	}))

	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		contentType string
		response    string
		status      int
	}{
		{
			name: "health", method: http.MethodGet, path: "/healthz", body: "", contentType: "",
			status: http.StatusOK, response: "ok\n",
		},
		{
			name: "preflight", method: http.MethodOptions, path: compilePath, body: "", contentType: "",
			status: http.StatusNoContent, response: "",
		},
		{
			name: compileCase, method: http.MethodPost, path: compilePath,
			body: `{"source":"package main"}`, contentType: jsonMediaType, status: http.StatusOK, response: "",
		},
		{
			name: "method", method: http.MethodGet, path: compilePath, body: "", contentType: "",
			status: http.StatusMethodNotAllowed, response: "method not allowed\n",
		},
		{
			name: "missing", method: http.MethodGet, path: "/missing", body: "", contentType: "",
			status: http.StatusNotFound, response: "404 page not found\n",
		},
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

	handler := newHandler(testConfigPointer(), compilerFunc(func(context.Context, string) ([]byte, error) {
		return nil, errors.New("unexpected compilation")
	}))

	tests := []struct {
		name        string
		body        string
		contentType string
		message     string
		status      int
	}{
		{
			name: "media type", body: `{}`, contentType: "text/plain",
			status: http.StatusUnsupportedMediaType, message: "Content-Type must be application/json",
		},
		{
			name: "malformed", body: `{`, contentType: jsonMediaType,
			status: http.StatusBadRequest, message: "invalid JSON request",
		},
		{
			name: "unknown field", body: `{"source":"x","other":1}`, contentType: jsonMediaType,
			status: http.StatusBadRequest, message: "invalid JSON request",
		},
		{
			name: "empty", body: `{"source":""}`, contentType: jsonMediaType,
			status: http.StatusBadRequest, message: "source is required",
		},
		{
			name: "source limit", body: `{"source":"abcdefghijklmnopqrstuvwxyz0123456789"}`,
			contentType: jsonMediaType, status: http.StatusRequestEntityTooLarge, message: "source is too large",
		},
		{
			name: "request limit", body: `{"source":"` + strings.Repeat("x", 200) + `"}`,
			contentType: jsonMediaType, status: http.StatusRequestEntityTooLarge, message: "request body is too large",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequestWithContext(
				t.Context(), http.MethodPost, compilePath, strings.NewReader(test.body),
			)
			request.Header.Set("Content-Type", test.contentType)

			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, test.status, response.Code)
			require.Contains(t, response.Result().Header.Get(contentTypeHeader), jsonMediaType)
			require.JSONEq(t, `{"error":`+strconv.Quote(test.message)+`}`, response.Body.String())
		})
	}
}

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
		response := httptest.NewRecorder()
		handler.ServeHTTP(
			response,
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
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, compileRequestForTest(t.Context()))

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
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, compileRequestForTest(t.Context()))

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

func compileRequestForTest(ctx context.Context) *http.Request {
	return compileRequestWithSource(ctx, "package main")
}

func compileRequestWithSource(
	ctx context.Context,
	source string,
) *http.Request {
	body, err := json.Marshal(compileRequest{Source: source})
	if err != nil {
		panic(err)
	}

	request := httptest.NewRequestWithContext(
		ctx,
		http.MethodPost,
		compilePath,
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", jsonMediaType)
	request.Header.Set("Origin", "https://play.example")

	return request
}
