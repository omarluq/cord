package serialization_test

import (
	"testing"

	"github.com/omarluq/cord/internal/serialization"
)

type benchmarkRecursiveRecord struct {
	Next   *benchmarkRecursiveRecord `json:"next,omitempty"`
	Labels map[string]string         `json:"labels"`
	Values []int                     `json:"values"`
}

func BenchmarkJSONCodec(b *testing.B) {
	benchmarks := []struct {
		value any
		name  string
	}{
		{name: "scalar", value: 42},
		{name: "record", value: benchmarkRecursiveRecord{
			Next: nil, Labels: map[string]string{"project": "cord"}, Values: []int{1, 2, 3, 4},
		}},
		{name: "recursive", value: benchmarkRecursiveRecord{
			Labels: map[string]string{"depth": "one"}, Values: nil,
			Next: &benchmarkRecursiveRecord{
				Next: nil, Labels: map[string]string{"depth": "two"}, Values: []int{1, 2, 3, 4},
			},
		}},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			switch value := benchmark.value.(type) {
			case int:
				benchmarkJSONCodecValue(b, value)
			case benchmarkRecursiveRecord:
				benchmarkJSONCodecValue(b, value)
			default:
				b.Fatalf("unsupported benchmark value %T", value)
			}
		})
	}
}

func benchmarkJSONCodecValue[T any](b *testing.B, value T) {
	b.Helper()
	b.Run("construct", benchmarkCodecConstruction[T])

	codec, err := serialization.NewJSONCodec[T]()
	if err != nil {
		b.Fatal(err)
	}

	payload, err := codec.Encode(value)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("encode", func(b *testing.B) { benchmarkCodecEncode(b, codec, value, payload) })
	b.Run("decode", func(b *testing.B) { benchmarkCodecDecode(b, codec, payload) })
	b.Run("fingerprint", func(b *testing.B) { benchmarkCodecFingerprint(b, codec) })
}

func benchmarkCodecConstruction[T any](b *testing.B) {
	b.Helper()
	b.ReportAllocs()

	for range b.N {
		if _, err := serialization.NewJSONCodec[T](); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCodecEncode[T any](b *testing.B, codec serialization.JSONCodec[T], value T, payload []byte) {
	b.Helper()
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()

	for range b.N {
		if _, err := codec.Encode(value); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCodecDecode[T any](b *testing.B, codec serialization.JSONCodec[T], payload []byte) {
	b.Helper()
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()

	for range b.N {
		if _, err := codec.Decode(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCodecFingerprint[T any](b *testing.B, codec serialization.JSONCodec[T]) {
	b.Helper()
	b.ReportAllocs()

	for range b.N {
		fingerprint, err := codec.TypeFingerprint()
		if err != nil {
			b.Fatal(err)
		}

		if len(fingerprint) != 64 {
			b.Fatalf("unexpected fingerprint length %d", len(fingerprint))
		}
	}
}
