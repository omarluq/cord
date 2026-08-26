package serialization

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/omarluq/cord/internal/hashframe"
)

const (
	fingerprintVersion      = "cord/fingerprint/v2"
	signatureFramingEntries = 4
)

// TypeFingerprint hashes a Go type identity together with its codec version.
// Generic instantiations are rejected because reflection does not expose their
// type arguments with enough package information to identify them canonically.
func TypeFingerprint(valueType reflect.Type, codecVersion string) (string, error) {
	if genericInstantiation(valueType) {
		return "", fmt.Errorf("fingerprint type %s: %w", valueType, errGenericInstantiation)
	}

	packagePath, typeName := typeIdentity(valueType)

	return hashframe.SHA256(
		fingerprintVersion,
		"type",
		codecVersion,
		packagePath,
		typeName,
		normalizedType(valueType),
	), nil
}

// SignatureFingerprint hashes ordered input and output type fingerprints.
func SignatureFingerprint(inputFingerprints []string, outputFingerprint string) string {
	parts := make([]string, 0, len(inputFingerprints)+signatureFramingEntries)
	parts = append(parts, fingerprintVersion, "signature", strconv.Itoa(len(inputFingerprints)))
	parts = append(parts, inputFingerprints...)
	parts = append(parts, outputFingerprint)

	return hashframe.SHA256(parts...)
}
