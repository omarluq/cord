package serialization_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type codecRecord struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type namer interface {
	Name() string
}

type interfaceRecord struct {
	Value string `json:"value"`
}

func (record *interfaceRecord) Name() string {
	return record.Value
}

func TestJSONCodec_RoundTrip(t *testing.T) {
	t.Parallel()

	codec := newJSONCodec[codecRecord](t)
	input := codecRecord{Name: "sample", Count: 3}

	payload, err := codec.Encode(input)
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"sample","count":3}`, string(payload))

	output, err := codec.Decode(payload)
	require.NoError(t, err)
	assert.Equal(t, input, output)
}

func TestJSONCodec_NilRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("pointer", func(t *testing.T) {
		t.Parallel()
		assertNilJSONRoundTrip(t, (*codecRecord)(nil), func(value *codecRecord) bool { return value == nil })
	})
	t.Run("map", func(t *testing.T) {
		t.Parallel()
		assertNilJSONRoundTrip(t, map[string]int(nil), func(value map[string]int) bool { return value == nil })
	})
	t.Run("slice", func(t *testing.T) {
		t.Parallel()
		assertNilJSONRoundTrip(t, []int(nil), func(value []int) bool { return value == nil })
	})
}

func assertNilJSONRoundTrip[T any](t *testing.T, input T, isNil func(T) bool) {
	t.Helper()

	codec := newJSONCodec[T](t)
	payload, err := codec.Encode(input)
	require.NoError(t, err)
	assert.Equal(t, []byte("null"), payload)
	assert.NotNil(t, payload)

	output, err := codec.Decode(payload)
	require.NoError(t, err)
	assert.True(t, isNil(output))
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

func jsonCodecConstructionError[T any]() error {
	_, err := serialization.NewJSONCodec[T]()

	return err
}

func TestJSONCodec_RejectsDynamicInterfaceValues(t *testing.T) {
	t.Parallel()

	var typedNil *interfaceRecord

	values := []struct {
		value any
		name  string
	}{
		{name: "dynamic struct", value: interfaceRecord{Value: "dynamic"}},
		{name: "integer", value: int64(9)},
		{name: "typed nil pointer", value: typedNil},
	}

	for _, test := range values {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var codec serialization.JSONCodec[any]

			payload, err := codec.Encode(test.value)
			require.Error(t, err)
			assert.Nil(t, payload)
			assert.ErrorContains(t, err, "interface types are not supported")
		})
	}

	var codec serialization.JSONCodec[namer]

	payload, err := codec.Encode(typedNil)
	require.Error(t, err)
	assert.Nil(t, payload)
	assert.ErrorContains(t, err, "interface types are not supported")
}

func TestJSONCodec_ZeroValueIsSafe(t *testing.T) {
	t.Parallel()

	var codec serialization.JSONCodec[codecRecord]

	fingerprint, err := codec.TypeFingerprint()
	require.NoError(t, err)
	assert.Len(t, fingerprint, 64)

	payload, err := codec.Encode(codecRecord{Name: "zero", Count: 0})
	require.NoError(t, err)
	decoded, err := codec.Decode(payload)
	require.NoError(t, err)
	assert.Equal(t, codecRecord{Name: "zero", Count: 0}, decoded)
}

func TestJSONCodec_RejectsNullForNonNilableType(t *testing.T) {
	t.Parallel()

	value, err := newJSONCodec[int](t).Decode([]byte(" null \n"))
	require.Error(t, err)
	assert.Zero(t, value)
}

func TestJSONCodec_Errors(t *testing.T) {
	t.Parallel()

	payload, err := newJSONCodec[float64](t).Encode(math.NaN())
	require.Error(t, err)
	assert.Nil(t, payload)
	require.ErrorAs(t, err, new(*json.UnsupportedValueError))

	value, err := newJSONCodec[codecRecord](t).Decode([]byte(`{"name":`))
	require.Error(t, err)
	assert.Zero(t, value)
	require.ErrorAs(t, err, new(*json.SyntaxError))
}

func newJSONCodec[T any](t *testing.T) serialization.JSONCodec[T] {
	t.Helper()

	codec, err := serialization.NewJSONCodec[T]()
	require.NoError(t, err)

	return codec
}
