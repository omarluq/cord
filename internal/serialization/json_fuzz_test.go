package serialization_test

import (
	"testing"
	"unicode/utf8"

	"github.com/omarluq/cord/internal/serialization"
)

// FuzzJSONCodec_StringRoundTrip verifies that strings survive JSON codec round trips.
func FuzzJSONCodec_StringRoundTrip(f *testing.F) {
	for _, seed := range []string{"", "cord", "workflow-世界", "\x00\n\t", "\xff\xfe"} {
		f.Add(seed)
	}

	codec, err := serialization.NewJSONCodec[string]()
	if err != nil {
		f.Fatalf("construct codec: %v", err)
	}

	f.Fuzz(func(t *testing.T, input string) {
		payload, encodeErr := codec.Encode(input)
		if encodeErr != nil {
			t.Fatalf("encode %q: %v", input, encodeErr)
		}

		output, decodeErr := codec.Decode(payload)
		if decodeErr != nil {
			t.Fatalf("decode encoded payload %q: %v", payload, decodeErr)
		}

		expected := input
		if !utf8.ValidString(input) {
			// encoding/json replaces each invalid UTF-8 byte with Unicode's
			// replacement rune when marshaling a string.
			expected = string([]rune(input))
		}

		if output != expected {
			t.Fatalf("round trip = %q, want %q", output, expected)
		}
	})
}

func FuzzJSONCodec_MalformedPayloadNeverPanics(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		[]byte("null"),
		[]byte(`{"name":"cord","count":1}`),
		[]byte(`{"name":`),
		{0xff, 0xfe, 0xfd},
	} {
		f.Add(seed)
	}

	codec, err := serialization.NewJSONCodec[codecRecord]()
	if err != nil {
		f.Fatalf("construct codec: %v", err)
	}

	f.Fuzz(func(t *testing.T, payload []byte) {
		// Decode errors are expected for arbitrary bytes; the invariant is that
		// malformed durable payloads are rejected without panicking.
		value, decodeErr := codec.Decode(payload)
		if decodeErr != nil {
			return
		}

		if _, encodeErr := codec.Encode(value); encodeErr != nil {
			t.Fatalf("re-encode decoded value: %v", encodeErr)
		}
	})
}
