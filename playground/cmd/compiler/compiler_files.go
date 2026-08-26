package main

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
)

const cordModule = "github.com/omarluq/cord"

func moduleSource(cordDirectory string) string {
	return fmt.Sprintf(`module playground.user

go 1.27.0

require %s v0.0.0

replace %s => %s
`, cordModule, cordModule, strconv.Quote(filepath.ToSlash(cordDirectory)))
}

func readWASM(directory string, maxBytes int64) ([]byte, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, fmt.Errorf("open compilation directory: %w", err)
	}
	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			slog.Warn("close compilation root", "error", closeErr)
		}
	}()

	outputFile, err := root.Open("app.wasm")
	if err != nil {
		return nil, fmt.Errorf("open WebAssembly: %w", err)
	}

	defer func() {
		if closeErr := outputFile.Close(); closeErr != nil {
			slog.Warn("close WebAssembly output", "error", closeErr)
		}
	}()

	wasm, err := io.ReadAll(io.LimitReader(outputFile, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read WebAssembly: %w", err)
	}

	if int64(len(wasm)) > maxBytes {
		return nil, fmt.Errorf("WebAssembly exceeds %d-byte limit", maxBytes)
	}

	return wasm, nil
}

type limitedBuffer struct {
	buffer    bytes.Buffer
	remaining int
	truncated bool
}

func newLimitedBuffer(limit int) *limitedBuffer {
	return &limitedBuffer{
		buffer:    bytes.Buffer{},
		remaining: limit,
		truncated: false,
	}
}

func (buffer *limitedBuffer) Write(value []byte) (int, error) {
	length := len(value)
	if length > buffer.remaining {
		value = value[:buffer.remaining]
		buffer.truncated = true
	}

	written, err := buffer.buffer.Write(value)
	if err != nil {
		return written, fmt.Errorf("buffer diagnostics: %w", err)
	}

	buffer.remaining -= written

	return length, nil
}

func (buffer *limitedBuffer) String() string {
	if buffer.truncated {
		return buffer.buffer.String() + "\n[compiler diagnostics truncated]"
	}

	return buffer.buffer.String()
}
