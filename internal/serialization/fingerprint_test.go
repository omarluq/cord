package serialization_test

import (
	"html/template"
	"reflect"
	"testing"
	texttemplate "text/template"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type namedNumber int

type goldenRecord struct {
	Value int `json:"value"`
}

type changedFieldRecord struct {
	Value string `json:"value"`
}

type changedTagRecord struct {
	Value int `json:"changed"`
}

type recursiveRecord struct {
	Next *recursiveRecord
}

func TestTypeFingerprint_IsStableAndTypeSpecific(t *testing.T) {
	t.Parallel()

	codec := newJSONCodec[codecRecord](t)
	fingerprint, err := codec.TypeFingerprint()
	require.NoError(t, err)
	comparisonCodec := newJSONCodec[codecRecord](t)
	comparisonFingerprint, err := comparisonCodec.TypeFingerprint()
	require.NoError(t, err)
	assert.Equal(t, fingerprint, comparisonFingerprint)

	tests := []struct {
		name       string
		valueType  reflect.Type
		codec      string
		comparison string
	}{
		{
			name:       "named and underlying types",
			valueType:  reflect.TypeFor[namedNumber](),
			codec:      serialization.JSONCodecVersion,
			comparison: mustFingerprint(t, reflect.TypeFor[int](), serialization.JSONCodecVersion),
		},
		{
			name:       "pointer and value types",
			valueType:  reflect.TypeFor[*codecRecord](),
			codec:      serialization.JSONCodecVersion,
			comparison: fingerprint,
		},
		{
			name:       "codec versions",
			valueType:  reflect.TypeFor[codecRecord](),
			codec:      "json/v2",
			comparison: fingerprint,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			actual := mustFingerprint(t, test.valueType, test.codec)
			assert.NotEqual(t, test.comparison, actual)
			assert.Len(t, actual, 64)
		})
	}
}

func TestTypeFingerprint_IncludesNamedTypeStructure(t *testing.T) {
	t.Parallel()

	baseline := mustFingerprint(t, reflect.TypeFor[goldenRecord](), serialization.JSONCodecVersion)
	tests := map[string]reflect.Type{
		"field type": reflect.TypeFor[changedFieldRecord](),
		"JSON tag":   reflect.TypeFor[changedTagRecord](),
	}

	for name, valueType := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.NotEqual(t, baseline, mustFingerprint(t, valueType, serialization.JSONCodecVersion))
		})
	}
}

func TestTypeFingerprint_SupportsRecursiveNamedTypes(t *testing.T) {
	t.Parallel()

	fingerprint := mustFingerprint(t, reflect.TypeFor[recursiveRecord](), serialization.JSONCodecVersion)
	assert.Len(t, fingerprint, 64)
	assert.Equal(t, fingerprint, mustFingerprint(t, reflect.TypeFor[recursiveRecord](), serialization.JSONCodecVersion))
}

func TestTypeFingerprint_DistinguishesSameNamedTypesFromDifferentPackages(t *testing.T) {
	t.Parallel()

	htmlFingerprint := mustFingerprint(t, reflect.TypeFor[template.Template](), serialization.JSONCodecVersion)
	textFingerprint := mustFingerprint(t, reflect.TypeFor[texttemplate.Template](), serialization.JSONCodecVersion)
	assert.NotEqual(t, htmlFingerprint, textFingerprint)
}

func mustFingerprint(t *testing.T, valueType reflect.Type, codecVersion string) string {
	t.Helper()

	fingerprint, err := serialization.TypeFingerprint(valueType, codecVersion)
	require.NoError(t, err)

	return fingerprint
}

func mustCodecFingerprint[T any](t *testing.T, codec serialization.JSONCodec[T]) string {
	t.Helper()

	fingerprint, err := codec.TypeFingerprint()
	require.NoError(t, err)

	return fingerprint
}
