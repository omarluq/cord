package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/require"
)

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
