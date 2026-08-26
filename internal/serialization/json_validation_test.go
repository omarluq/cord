package serialization_test

import (
	"encoding/json"
	"testing"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type namer interface {
	Name() string
}

type interfaceRecord struct {
	Value string `json:"value"`
}

func (record *interfaceRecord) Name() string {
	return record.Value
}

func TestNewJSONCodec_RejectsInterfaceTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		construct func() error
		name      string
	}{
		{name: "top-level interface", construct: jsonCodecConstructionError[any]},
		{name: "named interface", construct: jsonCodecConstructionError[namer]},
		{name: "interface in struct", construct: jsonCodecConstructionError[struct{ Value any }]},
		{name: "interface in slice", construct: jsonCodecConstructionError[[]any]},
		{name: "interface in array", construct: jsonCodecConstructionError[[1]any]},
		{name: "interface as map key", construct: jsonCodecConstructionError[map[any]string]},
		{name: "interface as map value", construct: jsonCodecConstructionError[map[string]any]},
		{name: "pointer to interface", construct: jsonCodecConstructionError[*any]},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := test.construct()
			require.Error(t, err)
			assert.ErrorContains(t, err, "interface types are not supported")
		})
	}
}

func TestNewJSONCodec_RejectsUnsupportedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		construct func() error
		name      string
	}{
		{name: "channel", construct: jsonCodecConstructionError[chan int]},
		{name: "function", construct: jsonCodecConstructionError[func()]},
		{name: "complex number", construct: jsonCodecConstructionError[complex128]},
		{name: "unsupported map key", construct: jsonCodecConstructionError[map[float64]string]},
		{name: "nested unsupported type", construct: jsonCodecConstructionError[struct{ Value chan int }]},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := test.construct()
			require.Error(t, err)
			require.ErrorAs(t, err, new(*json.UnsupportedTypeError))
		})
	}
}

// TestJSONCodec_RejectsDynamicInterfaceValues verifies that unsupported dynamic values are rejected.
func TestJSONCodec_RejectsDynamicInterfaceValues(t *testing.T) {
	t.Parallel()

	var typedNil *interfaceRecord

	values := []struct {
		encode func() ([]byte, error)
		name   string
	}{
		{name: "dynamic struct", encode: func() ([]byte, error) {
			var codec serialization.JSONCodec[any]

			return codec.Encode(interfaceRecord{Value: "dynamic"})
		}},
		{name: "integer", encode: func() ([]byte, error) {
			var codec serialization.JSONCodec[any]

			return codec.Encode(int64(9))
		}},
		{name: "typed nil pointer", encode: func() ([]byte, error) {
			var codec serialization.JSONCodec[any]

			return codec.Encode(typedNil)
		}},
		{name: "named interface codec", encode: func() ([]byte, error) {
			var codec serialization.JSONCodec[namer]

			return codec.Encode(typedNil)
		}},
	}

	for _, test := range values {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			payload, err := test.encode()
			require.Error(t, err)
			assert.Nil(t, payload)
			assert.ErrorContains(t, err, "interface types are not supported")
		})
	}
}

func jsonCodecConstructionError[T any]() error {
	_, err := serialization.NewJSONCodec[T]()

	return err
}
