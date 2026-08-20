package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
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

	artifact, err := newCompilationArtifact(protocol.Graph{}, []byte("wasm"))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	require.NoError(t, writeArtifact(response, &artifact, identityEncoding))
	require.Equal(t, strconv.Itoa(response.Body.Len()), response.Header().Get("Content-Length"))
	require.Empty(t, response.Header().Get("Content-Encoding"))
	require.Contains(t, response.Body.String(), "wasm")
}

func TestWriteArtifactGzip(t *testing.T) {
	t.Parallel()

	artifact, err := newCompilationArtifact(protocol.Graph{}, []byte(strings.Repeat("wasm", 1_000)))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	require.NoError(t, writeArtifact(response, &artifact, gzipEncoding))
	require.Equal(t, "gzip", response.Header().Get("Content-Encoding"))
	require.Equal(t, strconv.Itoa(response.Body.Len()), response.Header().Get("Content-Length"))

	reader, err := gzip.NewReader(response.Body)
	require.NoError(t, err)
	decoded, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Contains(t, string(decoded), strings.Repeat("wasm", 1_000))
}

func TestNegotiateEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		header     string
		encoding   string
		acceptable bool
	}{
		{name: "absent", header: "", encoding: identityEncoding, acceptable: true},
		{name: gzipEncoding, header: gzipEncoding, encoding: gzipEncoding, acceptable: true},
		{name: "case insensitive", header: "GZip", encoding: gzipEncoding, acceptable: true},
		{
			name: "gzip tied with identity", header: "gzip, identity",
			encoding: gzipEncoding, acceptable: true,
		},
		{name: "identity preferred", header: "gzip;q=0.5", encoding: identityEncoding, acceptable: true},
		{
			name: "gzip preferred", header: "gzip;q=0.8, identity;q=0.2",
			encoding: gzipEncoding, acceptable: true,
		},
		{name: "wildcard", header: "*", encoding: gzipEncoding, acceptable: true},
		{
			name: "explicit gzip exclusion beats wildcard", header: "gzip;q=0, *;q=1",
			encoding: identityEncoding, acceptable: true,
		},
		{name: "unknown encoding", header: "br", encoding: identityEncoding, acceptable: true},
		{name: "identity only", header: "gzip;q=0", encoding: identityEncoding, acceptable: true},
		{name: "none acceptable", header: "gzip;q=0, identity;q=0", encoding: "", acceptable: false},
		{name: "wildcard excludes identity", header: "*;q=0", encoding: "", acceptable: false},
		{name: "NaN quality", header: "gzip;q=NaN", encoding: identityEncoding, acceptable: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			encoding, acceptable := negotiateEncoding(test.header)
			require.Equal(t, test.encoding, encoding)
			require.Equal(t, test.acceptable, acceptable)
		})
	}
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

func TestHandlerNegotiatesGzip(t *testing.T) {
	t.Parallel()

	handler := newHandler(
		testConfigPointer(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			return []byte(strings.Repeat("wasm", 1_000)), nil
		}),
	)
	request := compileRequestForTest(t.Context())
	request.Header.Set("Accept-Encoding", "gzip")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "gzip", response.Header().Get("Content-Encoding"))
	require.Equal(t, strconv.Itoa(response.Body.Len()), response.Header().Get("Content-Length"))
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.ElementsMatch(t, []string{"Origin", "Accept-Encoding"}, response.Header().Values("Vary"))

	reader, err := gzip.NewReader(response.Body)
	require.NoError(t, err)
	decoded, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Contains(t, string(decoded), strings.Repeat("wasm", 1_000))
}

func TestHandlerRejectsUnacceptableEncodingWithoutCompiling(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	handler := newHandler(
		testConfigPointer(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			calls.Add(1)

			return []byte("wasm"), nil
		}),
	)
	request := compileRequestForTest(t.Context())
	request.Header.Set("Accept-Encoding", "gzip;q=0, identity;q=0")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusNotAcceptable, response.Code)
	require.Empty(t, response.Header().Get("Content-Encoding"))
	require.Equal(t, int32(0), calls.Load())
	require.JSONEq(t, `{"error":"no acceptable response encoding"}`, response.Body.String())
}

func TestHandlerDoesNotCompressHealthOrErrors(t *testing.T) {
	t.Parallel()

	handler := newHandler(
		testConfigPointer(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			return nil, errors.New("unexpected compilation")
		}),
	)

	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		contentType string
	}{
		{name: "health", method: http.MethodGet, path: "/healthz", body: "", contentType: ""},
		{
			name: "compiler error", method: http.MethodPost, path: compilePath,
			body: `{"source":"package main"}`, contentType: jsonMediaType,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequestWithContext(
				t.Context(), test.method, test.path, strings.NewReader(test.body),
			)
			request.Header.Set("Content-Type", test.contentType)
			request.Header.Set("Accept-Encoding", "gzip")

			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Empty(t, response.Header().Get("Content-Encoding"))
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
