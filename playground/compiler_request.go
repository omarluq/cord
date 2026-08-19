//go:build !js || !wasm

package playground

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
)

func performCompilationRequest(
	ctx context.Context,
	endpoint string,
	body []byte,
) (*http.Response, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("create compilation request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")

	response, err := (&http.Client{Timeout: compilationRequestTimeout}).Do(request)
	if err != nil {
		return nil, fmt.Errorf("send compilation request: %w", err)
	}

	return response, nil
}
