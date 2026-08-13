// Package serialization provides durable payload codecs and compatibility fingerprints.
package serialization

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// JSONCodecVersion identifies the durable format produced by JSONCodec.
const JSONCodecVersion = "json/v1"

var (
	errInterfaceType     = errors.New("interface types are not supported by the default JSON codec")
	errNullForNonNilable = errors.New("JSON null is invalid for non-nilable type")
)

// JSONCodec encodes and decodes one non-interface Go type using encoding/json.
// Its zero value is ready to use.
type JSONCodec[T any] struct{}

// NewJSONCodec creates a typed JSON codec.
// It rejects T when its reachable structure contains an interface because
// encoding/json cannot preserve arbitrary dynamic Go types, integer widths, or
// typed nil values when decoding an interface.
func NewJSONCodec[T any]() (JSONCodec[T], error) {
	codec := JSONCodec[T]{}
	if err := validateJSONType(reflect.TypeFor[T]()); err != nil {
		return codec, err
	}

	if _, err := codec.TypeFingerprint(); err != nil {
		return codec, err
	}

	return codec, nil
}

// Encode serializes value without truncating the resulting payload.
func (codec JSONCodec[T]) Encode(value T) ([]byte, error) {
	valueType := reflect.TypeFor[T]()
	if err := validateJSONType(valueType); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode JSON payload for %s: %w", valueType, err)
	}

	return payload, nil
}

// Decode deserializes payload directly into T.
func (codec JSONCodec[T]) Decode(payload []byte) (T, error) {
	var value T

	valueType := reflect.TypeFor[T]()
	if err := validateJSONType(valueType); err != nil {
		return value, err
	}

	if bytes.Equal(bytes.TrimSpace(payload), []byte("null")) && !nilable(valueType) {
		return value, fmt.Errorf("decode JSON payload for %s: %w", valueType, errNullForNonNilable)
	}

	if err := json.Unmarshal(payload, &value); err != nil {
		return value, fmt.Errorf("decode JSON payload for %s: %w", valueType, err)
	}

	return value, nil
}

func validateJSONType(valueType reflect.Type) error {
	if valueType == nil || containsInterface(valueType, make(map[reflect.Type]bool)) {
		return fmt.Errorf("construct default JSON codec for %s: %w", valueType, errInterfaceType)
	}

	return nil
}

func containsInterface(valueType reflect.Type, visited map[reflect.Type]bool) bool {
	if valueType.Kind() == reflect.Interface {
		return true
	}

	if visited[valueType] {
		return false
	}

	visited[valueType] = true

	for _, childType := range jsonChildTypes(valueType) {
		if containsInterface(childType, visited) {
			return true
		}
	}

	return false
}

func jsonChildTypes(valueType reflect.Type) []reflect.Type {
	switch valueType.Kind() {
	case reflect.Array, reflect.Pointer, reflect.Slice:
		return []reflect.Type{valueType.Elem()}
	case reflect.Map:
		return []reflect.Type{valueType.Key(), valueType.Elem()}
	case reflect.Struct:
		children := make([]reflect.Type, 0, valueType.NumField())
		for field := range valueType.Fields() {
			children = append(children, field.Type)
		}

		return children
	case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func, reflect.Interface,
		reflect.String, reflect.UnsafePointer:
		return nil
	}

	return nil
}

func nilable(valueType reflect.Type) bool {
	return valueType.Kind() == reflect.Chan || valueType.Kind() == reflect.Func ||
		valueType.Kind() == reflect.Map || valueType.Kind() == reflect.Pointer || valueType.Kind() == reflect.Slice
}

// TypeFingerprint returns the codec-specific fingerprint for T. It computes the
// value lazily, so a zero-value JSONCodec never yields an empty fingerprint.
func (codec JSONCodec[T]) TypeFingerprint() (string, error) {
	valueType := reflect.TypeFor[T]()
	if err := validateJSONType(valueType); err != nil {
		return "", err
	}

	return TypeFingerprint(valueType, JSONCodecVersion)
}
