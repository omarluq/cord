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
