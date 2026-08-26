package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/omarluq/cord/playground/internal/protocol"
)

func (service *service) readRequest(
	response http.ResponseWriter,
	request *http.Request,
) (protocol.CompileRequest, int, error) {
	body := http.MaxBytesReader(response, request.Body, service.maxRequest)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()

	input, status, err := decodeRequest(decoder)
	if err != nil {
		return protocol.CompileRequest{}, status, err
	}

	if err := ensureJSONEnd(decoder); err != nil {
		return protocol.CompileRequest{}, http.StatusBadRequest,
			errors.New("request must contain one JSON object")
	}

	return input, 0, nil
}

func (service *service) validSource(
	response http.ResponseWriter,
	source string,
) bool {
	if source == "" {
		writeJSONError(response, "source is required", http.StatusBadRequest)

		return false
	}

	if len(source) > service.maxSource {
		writeJSONError(response, "source is too large", http.StatusRequestEntityTooLarge)

		return false
	}

	return true
}

func decodeRequest(decoder *json.Decoder) (protocol.CompileRequest, int, error) {
	var input protocol.CompileRequest
	if err := decoder.Decode(&input); err != nil {
		maxBytesError, requestTooLarge := errors.AsType[*http.MaxBytesError](err)
		if requestTooLarge && maxBytesError != nil {
			return protocol.CompileRequest{}, http.StatusRequestEntityTooLarge,
				errors.New("request body is too large")
		}

		return protocol.CompileRequest{}, http.StatusBadRequest,
			errors.New("invalid JSON request")
	}

	return input, 0, nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("extra JSON value")
		}

		return fmt.Errorf("decode trailing JSON: %w", err)
	}

	return nil
}
