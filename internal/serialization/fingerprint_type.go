package serialization

import (
	"reflect"
	"strconv"
	"strings"
)

func writePart(destination *strings.Builder, part string) {
	destination.WriteString(strconv.Itoa(len(part)))
	destination.WriteByte(':')
	destination.WriteString(part)
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
