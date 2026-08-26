package playground

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/omarluq/cord/playground/internal/protocol"
)

const (
	compilationRequestTimeout = 3 * time.Minute
	maxCompilationErrorBytes  = 1 << 20
	maxGraphBytes             = 1 << 20
	maxWASMBytes              = 64 << 20
)

type compilationArtifact struct {
	graph protocol.Graph
	wasm  []byte
}

type compilationCache struct {
	source   string
	artifact compilationArtifact
	valid    bool
}

func (cache *compilationCache) get(source string) (compilationArtifact, bool) {
	if !cache.valid || cache.source != source {
		return compilationArtifact{}, false
	}

	return cache.artifact, true
}

func (cache *compilationCache) put(
	source string,
	artifact compilationArtifact,
) {
	cache.source = source
	cache.artifact = artifact
	cache.valid = true
}

func compile(
	ctx context.Context,
	endpoint string,
	source string,
) (compilationArtifact, error) {
	body, err := json.Marshal(protocol.CompileRequest{Source: source})
	if err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"encode compilation request: %w",
			err,
		)
	}

	response, err := performCompilationRequest(ctx, endpoint, body)
	if err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"compile workflow: %w",
			err,
		)
	}

	artifact, responseErr := readCompilationResponse(response)

	closeErr := response.Body.Close()
	if err := errors.Join(responseErr, closeErr); err != nil {
		return compilationArtifact{}, err
	}

	return artifact, nil
}

func readCompilationResponse(
	response *http.Response,
) (compilationArtifact, error) {
	if response.StatusCode != http.StatusOK {
		body, err := readLimited(
			response.Body,
			maxCompilationErrorBytes,
			"compilation error",
		)
		if err != nil {
			return compilationArtifact{}, err
		}

		return compilationArtifact{}, compilationError(response.Status, body)
	}

	artifact, err := decodeArtifact(
		response.Header.Get("Content-Type"),
		response.Body,
	)
	if err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"decode compilation response: %w",
			err,
		)
	}

	return artifact, nil
}

func compilationError(status string, body []byte) error {
	var failure protocol.ErrorResponse
	if json.Unmarshal(body, &failure) == nil && failure.Error != "" {
		return fmt.Errorf("compile workflow: %s", failure.Error)
	}

	return fmt.Errorf("compile workflow: %s", status)
}
