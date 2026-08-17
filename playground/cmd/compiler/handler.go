package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"
)

const (
	compilePath   = "/compile"
	jsonMediaType = "application/json"
)

type compileRequest struct {
	Source string `json:"source"`
}

type service struct {
	compiler      compiler
	allowedOrigin string
	maxRequest    int64
	maxSource     int
	timeout       time.Duration
	slots         chan struct{}
}

func newHandler(cfg config, compiler compiler) http.Handler {
	service := &service{
		compiler: compiler, allowedOrigin: cfg.allowedOrigin,
		maxRequest: cfg.maxRequestBytes, maxSource: cfg.maxSourceBytes,
		timeout: cfg.compileTimeout, slots: make(chan struct{}, cfg.maxConcurrency),
	}
	return http.HandlerFunc(service.serveHTTP)
}

func (service *service) serveHTTP(response http.ResponseWriter, request *http.Request) {
	service.setHeaders(response)
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

	body := http.MaxBytesReader(response, request.Body, service.maxRequest)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	input, status, err := decodeRequest(decoder)
	if err != nil {
		http.Error(response, err.Error(), status)
		return
	}
	if err := ensureJSONEnd(decoder); err != nil {
		http.Error(response, "request must contain one JSON object", http.StatusBadRequest)
		return
	}
	if !service.validSource(response, input.Source) {
		return
	}

	if !service.acquire(response) {
		return
	}
	defer func() { <-service.slots }()

	ctx, cancel := context.WithTimeout(request.Context(), service.timeout)
	defer cancel()
	wasm, err := service.compiler.Compile(ctx, input.Source)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeJSONError(response, "compilation timed out", http.StatusGatewayTimeout)
			return
		}
		writeJSONError(response, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	response.Header().Set("Content-Type", "application/wasm")
	response.Header().Set("Content-Disposition", `attachment; filename="app.wasm"`)
	response.WriteHeader(http.StatusOK)
	if _, err := response.Write(wasm); err != nil {
		return
	}
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

func (service *service) setHeaders(response http.ResponseWriter) {
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("X-Frame-Options", "DENY")
	response.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	response.Header().Set("Cache-Control", "no-store")
	if service.allowedOrigin != "" {
		response.Header().Set("Access-Control-Allow-Origin", service.allowedOrigin)
		response.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		response.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		response.Header().Set("Vary", "Origin")
	}
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
