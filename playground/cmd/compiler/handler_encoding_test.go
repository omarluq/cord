package main

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/require"
)

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
