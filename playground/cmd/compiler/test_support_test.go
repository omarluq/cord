package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
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
func testConfigPointer() *config {
	cfg := testConfig()

	return &cfg
}

func responseRecorder() (*httptest.ResponseRecorder, http.ResponseWriter) {
	response := httptest.NewRecorder()

	return response, newDeadlineWriter(response)
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
