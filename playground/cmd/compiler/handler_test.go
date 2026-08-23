package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

const compileRequestBody = `{"source":"package main"}`

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
		writeTimeout:    time.Second,
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
	writer := newDeadlineWriter(response)
	require.NoError(t, writeArtifact(writer, &artifact, identityEncoding, time.Second))
	require.Equal(t, strconv.Itoa(response.Body.Len()), response.Header().Get("Content-Length"))
	require.Empty(t, response.Header().Get("Content-Encoding"))
	require.Contains(t, response.Body.String(), "wasm")
}

type deadlineWriter struct {
	http.ResponseWriter
	deadline            time.Time
	deadlineErr         error
	wroteBeforeDeadline bool
}

func newDeadlineWriter(response http.ResponseWriter) *deadlineWriter {
	return &deadlineWriter{
		ResponseWriter:      response,
		deadline:            time.Time{},
		deadlineErr:         nil,
		wroteBeforeDeadline: false,
	}
}

func (writer *deadlineWriter) WriteHeader(statusCode int) {
	writer.recordWrite()
	writer.ResponseWriter.WriteHeader(statusCode)
}

func (writer *deadlineWriter) Write(content []byte) (int, error) {
	writer.recordWrite()

	written, err := writer.ResponseWriter.Write(content)
	if err != nil {
		return written, fmt.Errorf("write recorded response: %w", err)
	}

	return written, nil
}

func (writer *deadlineWriter) recordWrite() {
	writer.wroteBeforeDeadline = writer.wroteBeforeDeadline || writer.deadline.IsZero()
}

func (writer *deadlineWriter) SetWriteDeadline(deadline time.Time) error {
	writer.deadline = deadline

	return writer.deadlineErr
}

func TestWriteArtifactSetsDeadlineImmediatelyBeforeHeaders(t *testing.T) {
	t.Parallel()

	artifact, err := newCompilationArtifact(protocol.Graph{}, []byte("wasm"))
	require.NoError(t, err)

	writer := newDeadlineWriter(httptest.NewRecorder())
	require.NoError(t, writeArtifact(writer, &artifact, identityEncoding, time.Second))
	require.False(t, writer.deadline.IsZero())
	require.False(t, writer.wroteBeforeDeadline)
}

func TestWriteArtifactFailsClosedWithoutDeadlineSupport(t *testing.T) {
	t.Parallel()

	artifact, err := newCompilationArtifact(protocol.Graph{}, []byte("wasm"))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	err = writeArtifact(response, &artifact, identityEncoding, time.Second)
	require.ErrorIs(t, err, http.ErrNotSupported)
	require.Equal(t, http.StatusOK, response.Code)
	require.Empty(t, response.Header().Get("Content-Type"))
	require.Empty(t, response.Body.String())
}

func TestHandlerReturnsErrorWhenResponseDeadlinesAreUnsupported(t *testing.T) {
	t.Parallel()

	handler := newHandler(
		testConfigPointer(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			return []byte("wasm"), nil
		}),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, compileRequestForTest(t.Context()))

	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.JSONEq(t, `{"error":"compiler response unavailable"}`, response.Body.String())
	require.NotContains(t, response.Body.String(), "wasm")
}

func TestWriteArtifactReturnsDeadlineFailureBeforeHeaders(t *testing.T) {
	t.Parallel()

	artifact, err := newCompilationArtifact(protocol.Graph{}, []byte("wasm"))
	require.NoError(t, err)

	deadlineErr := errors.New("deadline failure")
	response := httptest.NewRecorder()
	writer := newDeadlineWriter(response)
	writer.deadlineErr = deadlineErr
	err = writeArtifact(writer, &artifact, identityEncoding, time.Second)
	require.ErrorIs(t, err, deadlineErr)
	require.Empty(t, response.Header().Get("Content-Type"))
	require.Empty(t, response.Body.String())
}

func TestHandlerBoundsStalledAndSlowReaders(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		readBytes int
	}{
		{name: "stalled", readBytes: 0},
		{name: "slow", readBytes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testHandlerBoundedReader(t, test.readBytes)
		})
	}
}

func testHandlerBoundedReader(t *testing.T, readBytes int) {
	t.Helper()

	cfg := testConfig()
	cfg.writeTimeout = 25 * time.Millisecond
	cfg.maxSourceBytes = 64
	artifact := bytes.Repeat([]byte("wasm"), 2<<20)
	handler := newHandler(&cfg, compilerFunc(func(context.Context, string) ([]byte, error) {
		return artifact, nil
	}))

	serverConnection, clientConnection := net.Pipe()
	handlerDone := make(chan struct{})
	server := &http.Server{
		Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			handler.ServeHTTP(response, request)
			close(handlerDone)
		}),
		ReadHeaderTimeout: time.Second,
	}
	listener := &singleConnectionListener{
		connection: serverConnection,
		closed:     make(chan struct{}),
		mu:         sync.Mutex{},
		accepted:   false,
		close:      sync.Once{},
	}
	serveDone := make(chan error, 1)

	go func() { serveDone <- server.Serve(listener) }()

	t.Cleanup(func() {
		require.NoError(t, clientConnection.Close())
		require.NoError(t, server.Close())
		require.ErrorIs(t, <-serveDone, http.ErrServerClosed)
	})

	const requestHeaders = "POST /compile HTTP/1.1\r\n" +
		"Host: compiler\r\nContent-Type: application/json\r\n" +
		"Content-Length: %d\r\nConnection: close\r\n\r\n%s"

	_, err := fmt.Fprintf(clientConnection, requestHeaders, len(compileRequestBody), compileRequestBody)
	require.NoError(t, err)

	if readBytes > 0 {
		go slowlyReadConnection(clientConnection, readBytes)
	}

	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("compiler server retained a response past its write deadline")
	}
}

func slowlyReadConnection(connection net.Conn, readBytes int) {
	buffer := make([]byte, readBytes)

	for {
		if _, err := io.ReadFull(connection, buffer); err != nil {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}
}

type singleConnectionListener struct {
	connection net.Conn
	closed     chan struct{}

	mu       sync.Mutex
	accepted bool
	close    sync.Once
}

func (listener *singleConnectionListener) Accept() (net.Conn, error) {
	listener.mu.Lock()
	if !listener.accepted {
		listener.accepted = true
		listener.mu.Unlock()

		return listener.connection, nil
	}
	listener.mu.Unlock()

	<-listener.closed

	return nil, net.ErrClosed
}

func (listener *singleConnectionListener) Close() error {
	listener.close.Do(func() { close(listener.closed) })

	return nil
}

func (*singleConnectionListener) Addr() net.Addr { return pipeAddress{} }

type pipeAddress struct{}

func (pipeAddress) Network() string { return "pipe" }

func (pipeAddress) String() string { return "pipe" }

func TestWriteArtifactGzip(t *testing.T) {
	t.Parallel()

	artifact, err := newCompilationArtifact(protocol.Graph{}, []byte(strings.Repeat("wasm", 1_000)))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	writer := newDeadlineWriter(response)
	require.NoError(t, writeArtifact(writer, &artifact, gzipEncoding, time.Second))
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

func responseRecorder() (*httptest.ResponseRecorder, http.ResponseWriter) {
	response := httptest.NewRecorder()

	return response, newDeadlineWriter(response)
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
			body: `{"source":"package main"}`, contentType: protocol.JSONMediaType, status: http.StatusOK, response: "",
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

			response, writer := responseRecorder()
			handler.ServeHTTP(writer, request)

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

	response, writer := responseRecorder()
	handler.ServeHTTP(writer, request)

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

func TestHandlerNegotiatesRepeatedAcceptEncodingHeaders(t *testing.T) {
	t.Parallel()

	handler := newHandler(
		testConfigPointer(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			return []byte("wasm"), nil
		}),
	)
	request := compileRequestForTest(t.Context())
	request.Header.Add("Accept-Encoding", "identity;q=0")
	request.Header.Add("Accept-Encoding", "gzip")

	response, writer := responseRecorder()
	handler.ServeHTTP(writer, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, gzipEncoding, response.Header().Get("Content-Encoding"))
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
			body: `{"source":"package main"}`, contentType: protocol.JSONMediaType,
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
			name: "malformed", body: `{`, contentType: protocol.JSONMediaType,
			status: http.StatusBadRequest, message: "invalid JSON request",
		},
		{
			name: "unknown field", body: `{"source":"x","other":1}`, contentType: protocol.JSONMediaType,
			status: http.StatusBadRequest, message: "invalid JSON request",
		},
		{
			name: "empty", body: `{"source":""}`, contentType: protocol.JSONMediaType,
			status: http.StatusBadRequest, message: "source is required",
		},
		{
			name:        "source limit",
			body:        `{"source":"abcdefghijklmnopqrstuvwxyz0123456789"}`,
			contentType: protocol.JSONMediaType,
			status:      http.StatusRequestEntityTooLarge,
			message:     "source is too large",
		},
		{
			name:        "request limit",
			body:        `{"source":"` + strings.Repeat("x", 200) + `"}`,
			contentType: protocol.JSONMediaType,
			status:      http.StatusRequestEntityTooLarge,
			message:     "request body is too large",
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
			require.Contains(t, response.Result().Header.Get(contentTypeHeader), protocol.JSONMediaType)
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

func compileRequestForTest(ctx context.Context) *http.Request {
	return compileRequestWithSource(ctx, "package main")
}

func compileRequestWithSource(
	ctx context.Context,
	source string,
) *http.Request {
	body, err := json.Marshal(protocol.CompileRequest{Source: source})
	if err != nil {
		panic(err)
	}

	request := httptest.NewRequestWithContext(
		ctx,
		http.MethodPost,
		compilePath,
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", protocol.JSONMediaType)
	request.Header.Set("Origin", "https://play.example")

	return request
}
