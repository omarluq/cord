package serialization_test

import (
	"html/template"
	"reflect"
	"testing"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type genericBox[T any] struct {
	Value T
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
