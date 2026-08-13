package serialization

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const (
	fingerprintVersion      = "cord/fingerprint/v2"
	signatureFramingEntries = 4
)

var errGenericInstantiation = errors.New("generic instantiated types are not supported for durable serialization")

// TypeFingerprint hashes a Go type identity together with its codec version.
// Generic instantiations are rejected because reflection does not expose their
// type arguments with enough package information to identify them canonically.
func TypeFingerprint(valueType reflect.Type, codecVersion string) (string, error) {
	if genericInstantiation(valueType) {
		return "", fmt.Errorf("fingerprint type %s: %w", valueType, errGenericInstantiation)
	}

	packagePath, typeName := typeIdentity(valueType)

	return hashParts(
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

	return hashParts(parts...)
}

func hashParts(parts ...string) string {
	var framed strings.Builder
	for _, part := range parts {
		writePart(&framed, part)
	}

	digest := sha256.Sum256([]byte(framed.String()))

	return hex.EncodeToString(digest[:])
}

func writePart(destination *strings.Builder, part string) {
	destination.WriteString(strconv.Itoa(len(part)))
	destination.WriteByte(':')
	destination.WriteString(part)
}

func genericInstantiation(valueType reflect.Type) bool {
	return genericInstantiationSeen(valueType, make(map[reflect.Type]bool))
}

func genericInstantiationSeen(valueType reflect.Type, seen map[reflect.Type]bool) bool {
	if valueType == nil || seen[valueType] {
		return false
	}

	seen[valueType] = true

	return namedGeneric(valueType) || nestedGeneric(valueType, seen)
}

func namedGeneric(valueType reflect.Type) bool {
	return valueType.Name() != "" && strings.Contains(valueType.Name(), "[")
}

func nestedGeneric(valueType reflect.Type, seen map[reflect.Type]bool) bool {
	switch valueType.Kind() {
	case reflect.Array, reflect.Chan, reflect.Pointer, reflect.Slice:
		return genericInstantiationSeen(valueType.Elem(), seen)
	case reflect.Func:
		return genericTypeList(valueType.Ins(), seen) || genericTypeList(valueType.Outs(), seen)
	case reflect.Interface:
		return genericInterface(valueType, seen)
	case reflect.Map:
		return genericInstantiationSeen(valueType.Key(), seen) || genericInstantiationSeen(valueType.Elem(), seen)
	case reflect.Struct:
		return genericStruct(valueType, seen)
	case reflect.Invalid, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.String, reflect.UnsafePointer:
		return false
	}

	return false
}

func genericTypeList(types func(func(reflect.Type) bool), seen map[reflect.Type]bool) bool {
	for valueType := range types {
		if genericInstantiationSeen(valueType, seen) {
			return true
		}
	}

	return false
}

func genericInterface(valueType reflect.Type, seen map[reflect.Type]bool) bool {
	for method := range valueType.Methods() {
		if genericInstantiationSeen(method.Type, seen) {
			return true
		}
	}

	return false
}

func genericStruct(valueType reflect.Type, seen map[reflect.Type]bool) bool {
	for field := range valueType.Fields() {
		if genericInstantiationSeen(field.Type, seen) {
			return true
		}
	}

	return false
}

func typeIdentity(valueType reflect.Type) (packagePath, typeName string) {
	if valueType == nil {
		return "", "nil"
	}

	if valueType.Name() != "" {
		return valueType.PkgPath(), valueType.Name()
	}

	return "", normalizedType(valueType)
}

func normalizedType(valueType reflect.Type) string {
	var identity strings.Builder
	writeType(&identity, valueType, make(map[reflect.Type]int))

	return identity.String()
}

func writeType(identity *strings.Builder, valueType reflect.Type, active map[reflect.Type]int) {
	if valueType == nil {
		identity.WriteString("nil")

		return
	}

	if reference, exists := active[valueType]; exists {
		identity.WriteString("ref(")
		identity.WriteString(strconv.Itoa(reference))
		identity.WriteByte(')')

		return
	}

	active[valueType] = len(active)
	defer delete(active, valueType)

	named := valueType.Name() != ""
	if named {
		identity.WriteString("named(")
		writePart(identity, valueType.PkgPath())
		writePart(identity, valueType.Name())
		identity.WriteByte(',')
	}

	writeTypeShape(identity, valueType, active)

	if named {
		identity.WriteByte(')')
	}
}

func writeTypeShape(identity *strings.Builder, valueType reflect.Type, active map[reflect.Type]int) {
	identity.WriteString(valueType.Kind().String())

	switch valueType.Kind() {
	case reflect.Array:
		identity.WriteByte('(')
		identity.WriteString(strconv.Itoa(valueType.Len()))
		identity.WriteByte(',')
		writeType(identity, valueType.Elem(), active)
		identity.WriteByte(')')
	case reflect.Chan:
		identity.WriteByte('(')
		identity.WriteString(strconv.Itoa(int(valueType.ChanDir())))
		identity.WriteByte(',')
		writeType(identity, valueType.Elem(), active)
		identity.WriteByte(')')
	case reflect.Func:
		writeFunctionType(identity, valueType, active)
	case reflect.Interface:
		writeInterfaceType(identity, valueType, active)
	case reflect.Map:
		identity.WriteByte('(')
		writeType(identity, valueType.Key(), active)
		identity.WriteByte(',')
		writeType(identity, valueType.Elem(), active)
		identity.WriteByte(')')
	case reflect.Pointer, reflect.Slice:
		identity.WriteByte('(')
		writeType(identity, valueType.Elem(), active)
		identity.WriteByte(')')
	case reflect.Struct:
		writeStructType(identity, valueType, active)
	case reflect.Invalid, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.String, reflect.UnsafePointer:
	}
}

func writeFunctionType(identity *strings.Builder, valueType reflect.Type, active map[reflect.Type]int) {
	identity.WriteByte('(')
	identity.WriteString(strconv.FormatBool(valueType.IsVariadic()))
	identity.WriteByte(';')
	writeTypeList(identity, valueType.NumIn(), valueType.In, active)
	identity.WriteByte(';')
	writeTypeList(identity, valueType.NumOut(), valueType.Out, active)
	identity.WriteByte(')')
}

func writeInterfaceType(identity *strings.Builder, valueType reflect.Type, active map[reflect.Type]int) {
	identity.WriteByte('(')

	for method := range valueType.Methods() {
		writePart(identity, method.PkgPath)
		writePart(identity, method.Name)
		writeType(identity, method.Type, active)
	}

	identity.WriteByte(')')
}

func writeStructType(identity *strings.Builder, valueType reflect.Type, active map[reflect.Type]int) {
	identity.WriteByte('(')

	for field := range valueType.Fields() {
		writePart(identity, field.PkgPath)
		writePart(identity, field.Name)
		writePart(identity, string(field.Tag))
		identity.WriteString(strconv.FormatBool(field.Anonymous))
		writeType(identity, field.Type, active)
	}

	identity.WriteByte(')')
}

func writeTypeList(
	identity *strings.Builder,
	count int,
	typeAt func(int) reflect.Type,
	active map[reflect.Type]int,
) {
	identity.WriteString(strconv.Itoa(count))
	identity.WriteByte('[')

	for index := range count {
		writeType(identity, typeAt(index), active)
	}

	identity.WriteByte(']')
}
