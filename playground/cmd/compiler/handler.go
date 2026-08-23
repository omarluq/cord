package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
)

const (
	compilePath       = "/compile"
	contentTypeHeader = "Content-Type"
	gzipEncoding      = "gzip"
	identityEncoding  = "identity"
)

var (
	errCompilerBusy          = errors.New("compiler is busy")
	errResponseWriteDeadline = errors.New("set compilation response write deadline")
)

type service struct {
	compiler       compiler
	allowedOrigins map[string]struct{}
	slots          chan struct{}
	cache          *compilationCache
	maxRequest     int64
	maxSource      int
	writeTimeout   time.Duration
}

func newHandler(cfg *config, compiler compiler) http.Handler {
	return newHandlerWithContext(context.Background(), cfg, compiler)
}

func newHandlerWithContext(
	ctx context.Context,
	cfg *config,
	compiler compiler,
) http.Handler {
	service := &service{
		compiler:       compiler,
		allowedOrigins: parseAllowedOrigins(cfg.allowedOrigin),
		maxRequest:     cfg.maxRequestBytes,
		maxSource:      cfg.maxSourceBytes,
		writeTimeout:   cfg.writeTimeout,
		slots:          make(chan struct{}, cfg.maxConcurrency),
		cache:          newCompilationCache(cfg),
	}

	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		service.serveHTTP(ctx, response, request)
	})
}

func (service *service) serveHTTP(
	serviceContext context.Context,
	response http.ResponseWriter,
	request *http.Request,
) {
	service.setHeaders(response, request.Header.Get("Origin"))

	if request.Method == http.MethodOptions {
		service.options(response, request)

		return
	}

	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/healthz":
		response.Header().Set(contentTypeHeader, "text/plain; charset=utf-8")
		response.WriteHeader(http.StatusOK)

		if _, err := response.Write([]byte("ok\n")); err != nil {
			slog.Error("write health response", "error", err)
		}
	case request.Method == http.MethodPost && request.URL.Path == compilePath:
		service.compile(serviceContext, response, request)
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

func (service *service) compile(
	serviceContext context.Context,
	response http.ResponseWriter,
	request *http.Request,
) {
	appendVary(response.Header(), "Accept-Encoding")

	mediaType, _, err := mime.ParseMediaType(request.Header.Get(contentTypeHeader))
	if err != nil || mediaType != protocol.JSONMediaType {
		writeJSONError(
			response,
			"Content-Type must be application/json",
			http.StatusUnsupportedMediaType,
		)

		return
	}

	input, status, err := service.readRequest(response, request)
	if err != nil {
		writeJSONError(response, err.Error(), status)

		return
	}

	if !service.validSource(response, input.Source) {
		return
	}

	encoding, acceptable := negotiateEncoding(strings.Join(request.Header.Values("Accept-Encoding"), ","))
	if !acceptable {
		writeJSONError(response, "no acceptable response encoding", http.StatusNotAcceptable)

		return
	}

	artifact, err := service.cache.load(
		serviceContext,
		request.Context(),
		input.Source,
		func(ctx context.Context) (compilationArtifact, error) {
			if !service.acquire() {
				return compilationArtifact{}, errCompilerBusy
			}
			defer func() { <-service.slots }()

			return service.compileSource(ctx, input.Source)
		},
	)
	if err != nil {
		service.writeCompileError(response, err)

		return
	}

	if err := writeArtifact(response, &artifact, encoding, service.writeTimeout); err != nil {
		slog.Error(
			"write compilation response",
			"error", err,
			"request_error", request.Context().Err(),
			"wasm_bytes", len(artifact.wasm),
		)

		if errors.Is(err, errResponseWriteDeadline) {
			writeJSONError(response, "compiler response unavailable", http.StatusInternalServerError)
		}
	}
}

func (service *service) compileSource(
	ctx context.Context,
	source string,
) (compilationArtifact, error) {
	graph, err := extractGraph(source)
	if err != nil {
		return compilationArtifact{}, err
	}

	instrumentedSource, err := instrumentWorkflow(source, graph)
	if err != nil {
		return compilationArtifact{}, err
	}

	wasm, err := service.compiler.Compile(ctx, instrumentedSource)
	if err != nil {
		return compilationArtifact{}, fmt.Errorf("compile workflow: %w", err)
	}

	return newCompilationArtifact(graph, wasm)
}

func (service *service) writeCompileError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errCompilerBusy):
		response.Header().Set("Retry-After", "1")
		writeJSONError(response, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, context.DeadlineExceeded):
		writeJSONError(response, "compilation timed out", http.StatusGatewayTimeout)
	default:
		writeJSONError(response, err.Error(), http.StatusUnprocessableEntity)
	}
}

func (service *service) readRequest(
	response http.ResponseWriter,
	request *http.Request,
) (protocol.CompileRequest, int, error) {
	body := http.MaxBytesReader(response, request.Body, service.maxRequest)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()

	input, status, err := decodeRequest(decoder)
	if err != nil {
		return protocol.CompileRequest{}, status, err
	}

	if err := ensureJSONEnd(decoder); err != nil {
		return protocol.CompileRequest{}, http.StatusBadRequest,
			errors.New("request must contain one JSON object")
	}

	return input, 0, nil
}

func newCompilationArtifact(
	graph protocol.Graph,
	wasm []byte,
) (compilationArtifact, error) {
	return compilationArtifact{
		graph:       graph,
		wasm:        wasm,
		boundary:    multipart.NewWriter(io.Discard).Boundary(),
		compression: &compressedRepresentation{},
	}, nil
}

func (artifact *compilationArtifact) gzipBody() ([]byte, error) {
	artifact.compression.once.Do(func() {
		var compressed bytes.Buffer

		compressor, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
		if err != nil {
			artifact.compression.err = fmt.Errorf("create gzip response: %w", err)

			return
		}

		if err := writeArtifactBody(compressor, artifact.boundary, artifact.graph, artifact.wasm); err != nil {
			artifact.compression.err = fmt.Errorf("compress compilation response: %w", err)

			return
		}

		if err := compressor.Close(); err != nil {
			artifact.compression.err = fmt.Errorf("finish gzip response: %w", err)

			return
		}

		artifact.compression.body = compressed.Bytes()
	})

	return artifact.compression.body, artifact.compression.err
}

func writeArtifact(
	response http.ResponseWriter,
	artifact *compilationArtifact,
	encoding string,
	writeTimeout time.Duration,
) error {
	var (
		gzipBody       []byte
		identityLength int64
	)

	if encoding == gzipEncoding {
		var err error

		gzipBody, err = artifact.gzipBody()
		if err != nil {
			return err
		}
	} else {
		counter := &byteCounter{}
		if err := writeArtifactBody(counter, artifact.boundary, artifact.graph, artifact.wasm); err != nil {
			return fmt.Errorf("measure compilation response: %w", err)
		}

		identityLength = counter.bytes
	}

	if err := http.NewResponseController(response).SetWriteDeadline(
		time.Now().Add(writeTimeout),
	); err != nil {
		// Fail closed before committing response headers. Proceeding through an
		// unsupported wrapper would make artifact writes unbounded.
		return fmt.Errorf("%w: %w", errResponseWriteDeadline, err)
	}

	response.Header().Set(
		contentTypeHeader,
		protocol.MultipartMediaType+"; boundary="+artifact.boundary,
	)
	response.Header().Set("Content-Disposition", `attachment; filename="cord-workflow"`)

	if encoding == gzipEncoding {
		response.Header().Set("Content-Encoding", encoding)
		response.Header().Set("Content-Length", strconv.Itoa(len(gzipBody)))
		response.WriteHeader(http.StatusOK)

		_, err := response.Write(gzipBody)
		if err != nil {
			return fmt.Errorf("write gzip compilation response: %w", err)
		}

		return nil
	}

	response.Header().Set("Content-Length", strconv.FormatInt(identityLength, 10))
	response.WriteHeader(http.StatusOK)

	return writeArtifactBody(response, artifact.boundary, artifact.graph, artifact.wasm)
}

type byteCounter struct {
	bytes int64
}

func (counter *byteCounter) Write(content []byte) (int, error) {
	counter.bytes += int64(len(content))

	return len(content), nil
}

func writeArtifactBody(
	output io.Writer,
	boundary string,
	graph protocol.Graph,
	wasm []byte,
) error {
	writer := multipart.NewWriter(output)
	if err := writer.SetBoundary(boundary); err != nil {
		return fmt.Errorf("set response boundary: %w", err)
	}

	graphPart, err := writer.CreateFormField(protocol.GraphPartName)
	if err != nil {
		return fmt.Errorf("create graph response: %w", err)
	}

	if encodeErr := json.NewEncoder(graphPart).Encode(graph); encodeErr != nil {
		return fmt.Errorf("encode graph response: %w", encodeErr)
	}

	wasmPart, err := writer.CreateFormFile(protocol.WASMPartName, "app.wasm")
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

func (service *service) validSource(
	response http.ResponseWriter,
	source string,
) bool {
	if source == "" {
		writeJSONError(response, "source is required", http.StatusBadRequest)

		return false
	}

	if len(source) > service.maxSource {
		writeJSONError(response, "source is too large", http.StatusRequestEntityTooLarge)

		return false
	}

	return true
}

func (service *service) acquire() bool {
	select {
	case service.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func decodeRequest(decoder *json.Decoder) (protocol.CompileRequest, int, error) {
	var input protocol.CompileRequest
	if err := decoder.Decode(&input); err != nil {
		maxBytesError, requestTooLarge := errors.AsType[*http.MaxBytesError](err)
		if requestTooLarge && maxBytesError != nil {
			return protocol.CompileRequest{}, http.StatusRequestEntityTooLarge,
				errors.New("request body is too large")
		}

		return protocol.CompileRequest{}, http.StatusBadRequest,
			errors.New("invalid JSON request")
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

func (service *service) setHeaders(
	response http.ResponseWriter,
	origin string,
) {
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("X-Frame-Options", "DENY")
	response.Header().Set(
		"Content-Security-Policy",
		"default-src 'none'; frame-ancestors 'none'",
	)
	response.Header().Set("Cache-Control", "no-store")

	if _, allowed := service.allowedOrigins[origin]; allowed {
		response.Header().Set("Access-Control-Allow-Origin", origin)
		response.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		response.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		appendVary(response.Header(), "Origin")
	}
}

func appendVary(header http.Header, field string) {
	for value := range strings.SplitSeq(header.Get("Vary"), ",") {
		if strings.EqualFold(strings.TrimSpace(value), field) {
			return
		}
	}

	header.Add("Vary", field)
}

func negotiateEncoding(value string) (string, bool) {
	if strings.TrimSpace(value) == "" {
		return identityEncoding, true
	}

	qualities := encodingQualities(value)
	gzipQuality := qualityFor(qualities, gzipEncoding, 0)
	identityQuality := identityQuality(qualities)

	if gzipQuality > 0 && gzipQuality >= identityQuality {
		return gzipEncoding, true
	}

	if identityQuality > 0 {
		return identityEncoding, true
	}

	return "", false
}

func encodingQualities(value string) map[string]float64 {
	qualities := make(map[string]float64)

	for item := range strings.SplitSeq(value, ",") {
		coding, quality, valid := parseEncoding(strings.TrimSpace(item))
		current, present := qualities[coding]

		if valid && (!present || quality > current) {
			qualities[coding] = quality
		}
	}

	return qualities
}

func identityQuality(qualities map[string]float64) float64 {
	if quality, present := qualities[identityEncoding]; present {
		return quality
	}

	if wildcard, present := qualities["*"]; present && wildcard == 0 {
		return 0
	}

	return 1
}

func qualityFor(qualities map[string]float64, coding string, defaultQuality float64) float64 {
	if quality, present := qualities[coding]; present {
		return quality
	}

	if quality, present := qualities["*"]; present {
		return quality
	}

	return defaultQuality
}

func parseEncoding(value string) (coding string, quality float64, valid bool) {
	parts := strings.Split(value, ";")
	coding = strings.ToLower(strings.TrimSpace(parts[0]))

	if coding == "" {
		return "", 0, false
	}

	quality = 1

	for _, parameter := range parts[1:] {
		name, raw, found := strings.Cut(strings.TrimSpace(parameter), "=")
		if !found || !strings.EqualFold(strings.TrimSpace(name), "q") {
			return "", 0, false
		}

		parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil || math.IsNaN(parsed) || parsed < 0 || parsed > 1 {
			return "", 0, false
		}

		quality = parsed
	}

	return coding, quality, true
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
	response.Header().Set(contentTypeHeader, protocol.JSONMediaType)
	response.WriteHeader(status)

	if err := json.NewEncoder(response).Encode(
		protocol.ErrorResponse{Error: message},
	); err != nil {
		slog.Error("write JSON error response", "error", err)
	}
}

func allowedMethods(path string) string {
	if path == compilePath {
		return "POST, OPTIONS"
	}

	return "GET"
}
