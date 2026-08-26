package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/require"
)

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
