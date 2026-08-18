package playground

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"time"

	goapp "github.com/maxence-charriere/go-app/v10/pkg/app"
	"github.com/omarluq/cord/playground/internal/protocol"
)

const compilationRequestTimeout = 3 * time.Minute

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
	ctx goapp.Context,
	endpoint string,
	source string,
) (compilationArtifact, error) {
	body, err := json.Marshal(map[string]string{"source": source})
	if err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"encode compilation request: %w",
			err,
		)
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"create compilation request: %w",
			err,
		)
	}
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: compilationRequestTimeout}
	response, err := client.Do(request)
	if err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"compile workflow: %w",
			err,
		)
	}
	result, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"read compilation response: %w",
			err,
		)
	}
	if response.StatusCode != http.StatusOK {
		return compilationArtifact{}, compilationError(
			response.Status,
			result,
		)
	}

	artifact, err := decodeArtifact(
		response.Header.Get("Content-Type"),
		result,
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
	var failure struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &failure) == nil && failure.Error != "" {
		return fmt.Errorf("compile workflow: %s", failure.Error)
	}
	return fmt.Errorf("compile workflow: %s", status)
}

func decodeArtifact(
	contentType string,
	body []byte,
) (compilationArtifact, error) {
	mediaType, parameters, err := mime.ParseMediaType(contentType)
	if err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"parse content type: %w",
			err,
		)
	}
	if mediaType != "multipart/form-data" ||
		parameters["boundary"] == "" {
		return compilationArtifact{}, fmt.Errorf(
			"unexpected content type %q",
			mediaType,
		)
	}

	artifact := compilationArtifact{
		graph: protocol.Graph{
			Nodes: []protocol.Node{},
			Edges: []protocol.Edge{},
		},
		wasm: []byte{},
	}
	reader := multipart.NewReader(
		bytes.NewReader(body),
		parameters["boundary"],
	)
	for {
		finished, err := readArtifactPart(reader, &artifact)
		if err != nil {
			return compilationArtifact{}, err
		}
		if finished {
			break
		}
	}
	if len(artifact.wasm) == 0 {
		return compilationArtifact{}, errors.New(
			"WebAssembly part is missing",
		)
	}
	return artifact, nil
}

func readArtifactPart(
	reader *multipart.Reader,
	artifact *compilationArtifact,
) (bool, error) {
	part, err := reader.NextPart()
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read part: %w", err)
	}
	content, readErr := io.ReadAll(part)
	closeErr := part.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return false, fmt.Errorf(
			"read %s part: %w",
			part.FormName(),
			err,
		)
	}

	switch part.FormName() {
	case "graph":
		if err := json.Unmarshal(content, &artifact.graph); err != nil {
			return false, fmt.Errorf("decode graph: %w", err)
		}
	case "wasm":
		artifact.wasm = content
	}
	return false, nil
}
