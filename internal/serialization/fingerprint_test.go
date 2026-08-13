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

type genericBox[T any] struct {
	Value T
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

func TestTypeFingerprint_RejectsGenericInstantiations(t *testing.T) {
	t.Parallel()

	tests := map[string]reflect.Type{
		"named":           reflect.TypeFor[genericBox[template.Template]](),
		"slice":           reflect.TypeFor[[]genericBox[template.Template]](),
		"map key":         reflect.TypeFor[map[genericBox[int]]string](),
		"channel element": reflect.TypeFor[chan genericBox[int]](),
		"function input":  reflect.TypeFor[func(genericBox[int])](),
		"struct field":    reflect.TypeFor[struct{ Box genericBox[int] }](),
		"interface method": reflect.TypeOf((*interface {
			Get() genericBox[int]
		})(nil)).Elem(),
	}
	for name, valueType := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fingerprint, err := serialization.TypeFingerprint(valueType, serialization.JSONCodecVersion)
			require.Error(t, err)
			assert.Empty(t, fingerprint)
			assert.ErrorContains(t, err, "generic instantiated types are not supported")
		})
	}
}

func TestTypeFingerprint_DistinguishesCompositeTypeShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		left  reflect.Type
		right reflect.Type
		name  string
	}{
		{name: "array length", left: reflect.TypeFor[[1]int](), right: reflect.TypeFor[[2]int]()},
		{name: "channel direction", left: reflect.TypeFor[chan int](), right: reflect.TypeFor[<-chan int]()},
		{name: "map key", left: reflect.TypeFor[map[int]string](), right: reflect.TypeFor[map[bool]string]()},
		{name: "pointer and slice", left: reflect.TypeFor[*int](), right: reflect.TypeFor[[]int]()},
		{
			name:  "function variadic",
			left:  reflect.TypeFor[func(...int) string](),
			right: reflect.TypeFor[func([]int) string](),
		},
		{name: "function output", left: reflect.TypeFor[func() int](), right: reflect.TypeFor[func() string]()},
		{
			name:  "interface method",
			left:  reflect.TypeOf((*interface{ Read() string })(nil)).Elem(),
			right: reflect.TypeOf((*interface{ Write() string })(nil)).Elem(),
		},
		{name: "struct field", left: reflect.TypeFor[struct{ A int }](), right: reflect.TypeFor[struct{ B int }]()},
		{name: "struct tag", left: reflect.TypeFor[struct {
			A int `json:"a"`
		}](), right: reflect.TypeFor[struct {
			A int `json:"b"`
		}]()},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			left := mustFingerprint(t, testCase.left, serialization.JSONCodecVersion)
			right := mustFingerprint(t, testCase.right, serialization.JSONCodecVersion)
			assert.NotEqual(t, left, right)
		})
	}
}

func TestFingerprints_GoldenCompatibility(t *testing.T) {
	t.Parallel()

	named := mustFingerprint(t, reflect.TypeFor[goldenRecord](), serialization.JSONCodecVersion)
	composite := mustFingerprint(t, reflect.TypeFor[[]*goldenRecord](), serialization.JSONCodecVersion)
	signature := serialization.SignatureFingerprint([]string{named, composite}, named)

	assert.Equal(t, "df4bd86e6db7de7f4f0d7ed9f4331c7ff53c60621c7ea2516d8d79f5cd18ffd0", named)
	assert.Equal(t, "0ee5e56f8949b3f540098d04a688a334475f9bc859f88e0fbf9a6c5a1f286dab", composite)
	assert.Equal(t, "fcef8af42a307b41f69d2e0e8deb38ca7a1b3ec145748197b16df41b44898486", signature)
}

func TestSignatureFingerprint_PreservesInputOrder(t *testing.T) {
	t.Parallel()

	integer := mustCodecFingerprint(t, newJSONCodec[int](t))
	text := mustCodecFingerprint(t, newJSONCodec[string](t))
	output := mustCodecFingerprint(t, newJSONCodec[bool](t))

	forward := serialization.SignatureFingerprint([]string{integer, text}, output)
	reverse := serialization.SignatureFingerprint([]string{text, integer}, output)

	assert.NotEqual(t, forward, reverse)
	assert.Equal(t, forward, serialization.SignatureFingerprint([]string{integer, text}, output))
	assert.Len(t, forward, 64)
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
