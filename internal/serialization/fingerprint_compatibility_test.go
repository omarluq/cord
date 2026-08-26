package serialization_test

import (
	"reflect"
	"testing"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/stretchr/testify/assert"
)

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
