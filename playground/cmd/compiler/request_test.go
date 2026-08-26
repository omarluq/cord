package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/require"
)

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
