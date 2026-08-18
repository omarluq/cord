package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
)

const (
	compilePath   = "/compile"
	jsonMediaType = "application/json"
)

type compileRequest struct {
	Source string `json:"source"`
}

type service struct {
	compiler       compiler
	allowedOrigins map[string]struct{}
	maxRequest     int64
	maxSource      int
	timeout        time.Duration
	slots          chan struct{}
	cache          *compilationCache
}

func newHandler(cfg config, compiler compiler) http.Handler {
	service := &service{
		compiler: compiler, allowedOrigins: parseAllowedOrigins(cfg.allowedOrigin),
		maxRequest: cfg.maxRequestBytes, maxSource: cfg.maxSourceBytes,
		timeout: cfg.compileTimeout,
		slots:   make(chan struct{}, cfg.maxConcurrency),
		cache:   newCompilationCache(cfg),
	}
	return http.HandlerFunc(service.serveHTTP)
}

func (service *service) serveHTTP(response http.ResponseWriter, request *http.Request) {
	service.setHeaders(response, request.Header.Get("Origin"))
	if request.Method == http.MethodOptions {
		service.options(response, request)
		return
	}

	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/healthz":
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.WriteHeader(http.StatusOK)
		if _, err := response.Write([]byte("ok\n")); err != nil {
			return
		}
	case request.Method == http.MethodPost && request.URL.Path == compilePath:
		service.compile(response, request)
	case request.URL.Path == compilePath || request.URL.Path == "/healthz":
		response.Header().Set("Allow", allowedMethods(request.URL.Path))
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
	default:
		http.NotFound(response, request)
	}
}

func (service *service) options(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path != compilePath {
		http.NotFound(response, request)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (service *service) compile(response http.ResponseWriter, request *http.Request) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != jsonMediaType {
		http.Error(response, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	input, status, err := service.readRequest(response, request)
	if err != nil {
		http.Error(response, err.Error(), status)
		return
	}
	if !service.validSource(response, input.Source) {
		return
	}
	if service.writeCached(response, input.Source) {
		return
	}

	if !service.acquire(response) {
		return
	}
	defer func() { <-service.slots }()

	ctx, cancel := context.WithTimeout(
		request.Context(),
		service.timeout,
	)
	defer cancel()
	service.compileSource(ctx, response, input.Source)
}

func (service *service) writeCached(
	response http.ResponseWriter,
	source string,
) bool {
	artifact, found := service.cache.get(source)
	if !found {
		return false
	}
	if err := writeArtifact(
		response,
		artifact.graph,
		artifact.wasm,
	); err != nil {
		return true
	}
	return true
}

func (service *service) compileSource(
	ctx context.Context,
	response http.ResponseWriter,
	source string,
) {
	graph, err := extractGraph(source)
	if err != nil {
		writeJSONError(response, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	instrumentedSource, err := instrumentWorkflow(source, graph)
	if err != nil {
		writeJSONError(response, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	wasm, err := service.compiler.Compile(ctx, instrumentedSource)
	if err != nil {
		writeCompilationError(response, err)
		return
	}

	service.cache.put(source, compilationArtifact{
		graph: graph,
		wasm:  wasm,
	})
	if err := writeArtifact(response, graph, wasm); err != nil {
		return
	}
}

func writeCompilationError(response http.ResponseWriter, err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		writeJSONError(response, "compilation timed out", http.StatusGatewayTimeout)
		return
	}
	writeJSONError(response, err.Error(), http.StatusUnprocessableEntity)
}

func (service *service) readRequest(response http.ResponseWriter, request *http.Request) (compileRequest, int, error) {
	body := http.MaxBytesReader(response, request.Body, service.maxRequest)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	input, status, err := decodeRequest(decoder)
	if err != nil {
		return compileRequest{}, status, err
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return compileRequest{}, http.StatusBadRequest, errors.New("request must contain one JSON object")
	}
	return input, 0, nil
}

func writeArtifact(response http.ResponseWriter, graph protocol.Graph, wasm []byte) error {
	writer := multipart.NewWriter(response)
	response.Header().Set("Content-Type", writer.FormDataContentType())
	response.Header().Set("Content-Disposition", `attachment; filename="cord-workflow"`)
	response.WriteHeader(http.StatusOK)

	graphPart, err := writer.CreateFormField("graph")
	if err != nil {
		return fmt.Errorf("create graph response: %w", err)
	}
	if err := json.NewEncoder(graphPart).Encode(graph); err != nil {
		return fmt.Errorf("encode graph response: %w", err)
	}

	wasmPart, err := writer.CreateFormFile("wasm", "app.wasm")
	if err != nil {
		return fmt.Errorf("create WebAssembly response: %w", err)
	}
	if _, err := wasmPart.Write(wasm); err != nil {
		return fmt.Errorf("write WebAssembly response: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("finish compilation response: %w", err)
	}
	return nil
}

func (service *service) validSource(response http.ResponseWriter, source string) bool {
	if source == "" {
		http.Error(response, "source is required", http.StatusBadRequest)
		return false
	}
	if len(source) > service.maxSource {
		http.Error(response, "source is too large", http.StatusRequestEntityTooLarge)
		return false
	}
	return true
}

func (service *service) acquire(response http.ResponseWriter) bool {
	select {
	case service.slots <- struct{}{}:
		return true
	default:
		response.Header().Set("Retry-After", "1")
		http.Error(response, "compiler is busy", http.StatusServiceUnavailable)
		return false
	}
}

func decodeRequest(decoder *json.Decoder) (compileRequest, int, error) {
	var input compileRequest
	if err := decoder.Decode(&input); err != nil {
		tooLarge, _ := errors.AsType[*http.MaxBytesError](err)
		if tooLarge != nil {
			return compileRequest{}, http.StatusRequestEntityTooLarge, errors.New("invalid JSON request")
		}
		return compileRequest{}, http.StatusBadRequest, errors.New("invalid JSON request")
	}
	return input, 0, nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("extra JSON value")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return nil
}

func (service *service) setHeaders(response http.ResponseWriter, origin string) {
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("X-Frame-Options", "DENY")
	response.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	response.Header().Set("Cache-Control", "no-store")
	if _, allowed := service.allowedOrigins[origin]; allowed {
		response.Header().Set("Access-Control-Allow-Origin", origin)
		response.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		response.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		response.Header().Set("Vary", "Origin")
	}
}

func parseAllowedOrigins(value string) map[string]struct{} {
	origins := make(map[string]struct{})
	for origin := range strings.SplitSeq(value, ",") {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			origins[origin] = struct{}{}
		}
	}
	return origins
}

func writeJSONError(response http.ResponseWriter, message string, status int) {
	response.Header().Set("Content-Type", jsonMediaType)
	response.WriteHeader(status)
	if err := json.NewEncoder(response).Encode(map[string]string{"error": message}); err != nil {
		return
	}
}

func allowedMethods(path string) string {
	if path == compilePath {
		return "POST, OPTIONS"
	}
	return "GET"
}
