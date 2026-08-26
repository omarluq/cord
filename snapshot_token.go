package cord

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	nodePageTokenMaxLen = 8192
	nodePageTokenV1     = byte(1)
)

type nodePageToken struct {
	RunID      RunID          `json:"run_id"`
	State      NodeState      `json:"state,omitempty"`
	Reason     TerminalReason `json:"reason,omitempty"`
	LastNodeID NodeID         `json:"last_node_id"`
}

type nodePageTokenWire struct {
	Payload  nodePageToken `json:"payload"`
	Checksum string        `json:"checksum"`
	Version  int           `json:"version"`
}

func encodeNodePageToken(token nodePageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", fmt.Errorf("encode token payload: %w", err)
	}

	checksum := sha256.Sum256(payload)
	wire := nodePageTokenWire{
		Payload: token, Checksum: base64.RawURLEncoding.EncodeToString(checksum[:]),
		Version: int(nodePageTokenV1),
	}

	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("encode token envelope: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeNodePageToken(encoded string) (nodePageToken, error) {
	wire, err := decodeNodePageTokenWire(encoded)
	if err != nil {
		return nodePageToken{}, err
	}

	if wire.Version != int(nodePageTokenV1) {
		return nodePageToken{}, fmt.Errorf("token version %d is unsupported", wire.Version)
	}

	if wire.Payload.RunID == "" || wire.Payload.LastNodeID == "" {
		return nodePageToken{}, errors.New("token cursor identity is empty")
	}

	if err := validateNodePageTokenChecksum(&wire); err != nil {
		return nodePageToken{}, err
	}

	return wire.Payload, nil
}

func decodeNodePageTokenWire(encoded string) (nodePageTokenWire, error) {
	if encoded == "" || len(encoded) > nodePageTokenMaxLen {
		return nodePageTokenWire{}, errors.New("token length is invalid")
	}

	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nodePageTokenWire{}, errors.New("token encoding is invalid")
	}

	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()

	var wire nodePageTokenWire
	if err = decoder.Decode(&wire); err != nil {
		return nodePageTokenWire{}, errors.New("token payload is invalid")
	}

	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nodePageTokenWire{}, errors.New("token payload has trailing data")
	}

	return wire, nil
}

func validateNodePageTokenChecksum(wire *nodePageTokenWire) error {
	payload, err := json.Marshal(wire.Payload)
	if err != nil {
		return errors.New("token payload is invalid")
	}

	expectedChecksum := sha256.Sum256(payload)

	providedChecksum, err := base64.RawURLEncoding.DecodeString(wire.Checksum)
	if err != nil || len(providedChecksum) != sha256.Size ||
		subtle.ConstantTimeCompare(providedChecksum, expectedChecksum[:]) != 1 {
		return errors.New("token checksum is invalid")
	}

	return nil
}
