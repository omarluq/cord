package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
)

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
