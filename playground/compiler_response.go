package playground

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"

	"github.com/omarluq/cord/playground/internal/protocol"
)

func decodeArtifact(
	contentType string,
	body io.Reader,
) (compilationArtifact, error) {
	mediaType, parameters, err := mime.ParseMediaType(contentType)
	if err != nil {
		return compilationArtifact{}, fmt.Errorf(
			"parse content type: %w",
			err,
		)
	}

	if mediaType != protocol.MultipartMediaType ||
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

	reader := multipart.NewReader(body, parameters["boundary"])
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

	name := part.FormName()

	limit := int64(maxGraphBytes)
	if name == protocol.WASMPartName {
		limit = maxWASMBytes
	}

	content, readErr := readLimited(part, limit, name)

	closeErr := part.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return false, fmt.Errorf("read %s part: %w", name, err)
	}

	switch name {
	case protocol.GraphPartName:
		if err := json.Unmarshal(content, &artifact.graph); err != nil {
			return false, fmt.Errorf("decode graph: %w", err)
		}
	case protocol.WASMPartName:
		artifact.wasm = content
	}

	return false, nil
}

func readLimited(
	reader io.Reader,
	limit int64,
	name string,
) ([]byte, error) {
	content, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}

	if int64(len(content)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", name, limit)
	}

	return content, nil
}
