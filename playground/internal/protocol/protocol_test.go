package protocol_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"testing"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestWireContractRoundTrip(t *testing.T) {
	t.Parallel()

	require.Equal(t, "application/json", protocol.JSONMediaType)
	require.Equal(t, "multipart/form-data", protocol.MultipartMediaType)
	require.Equal(t, "graph", protocol.GraphPartName)
	require.Equal(t, "wasm", protocol.WASMPartName)

	t.Run("request", func(t *testing.T) {
		t.Parallel()

		want := protocol.CompileRequest{Source: "package main"}
		encoded, err := json.Marshal(want)
		require.NoError(t, err)
		require.JSONEq(t, `{"source":"package main"}`, string(encoded))

		var got protocol.CompileRequest
		require.NoError(t, json.Unmarshal(encoded, &got))
		require.Equal(t, want, got)
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		want := protocol.ErrorResponse{Error: "invalid workflow"}
		encoded, err := json.Marshal(want)
		require.NoError(t, err)
		require.JSONEq(t, `{"error":"invalid workflow"}`, string(encoded))

		var got protocol.ErrorResponse
		require.NoError(t, json.Unmarshal(encoded, &got))
		require.Equal(t, want, got)
	})

	t.Run("artifact", testArtifactRoundTrip)
}

func testArtifactRoundTrip(t *testing.T) {
	t.Parallel()

	wantGraph := protocol.Graph{
		Nodes: []protocol.Node{{ID: "node-1", Label: "step"}},
		Edges: []protocol.Edge{},
	}
	wantWASM := []byte("\x00asm")

	var body bytes.Buffer

	writer := multipart.NewWriter(&body)

	graphPart, err := writer.CreateFormField(protocol.GraphPartName)
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(graphPart).Encode(wantGraph))

	wasmPart, err := writer.CreateFormFile(protocol.WASMPartName, "app.wasm")
	require.NoError(t, err)
	_, err = wasmPart.Write(wantWASM)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	mediaType, parameters, err := mime.ParseMediaType(writer.FormDataContentType())
	require.NoError(t, err)
	require.Equal(t, protocol.MultipartMediaType, mediaType)

	gotGraph, gotWASM := decodeArtifact(t, &body, parameters["boundary"])
	require.Equal(t, wantGraph, gotGraph)
	require.Equal(t, wantWASM, gotWASM)
}

func decodeArtifact(
	t *testing.T,
	body io.Reader,
	boundary string,
) (graph protocol.Graph, wasm []byte) {
	t.Helper()

	reader := multipart.NewReader(body, boundary)

	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)

		switch part.FormName() {
		case protocol.GraphPartName:
			require.NoError(t, json.NewDecoder(part).Decode(&graph))
		case protocol.WASMPartName:
			wasm, err = io.ReadAll(part)
			require.NoError(t, err)
		default:
			t.Fatalf("unexpected artifact part %q", part.FormName())
		}

		require.NoError(t, part.Close())
	}

	return graph, wasm
}
