package serialization

import (
	"errors"
	"reflect"
	"strings"
)

var errGenericInstantiation = errors.New("generic instantiated types are not supported for persisted serialization")

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
