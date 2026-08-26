package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
)

const compilePath = "/compile"

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

func (service *service) acquire() bool {
	select {
	case service.slots <- struct{}{}:
		return true
	default:
		return false
	}
}
