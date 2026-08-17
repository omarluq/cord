package sqlite

import (
	"errors"
	"reflect"
	"slices"
)

const (
	busyCode        = uint64(5)
	primaryCodeMask = uint64(0xff)
)

func isBusy(err error) bool {
	if err == nil {
		return false
	}

	if busyCodeMatches(err) {
		return true
	}

	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return slices.ContainsFunc(joined.Unwrap(), isBusy)
	}

	return isBusy(errors.Unwrap(err))
}

func busyCodeMatches(err error) bool {
	original := reflect.ValueOf(err)

	value, typeOf, ok := indirectValue(original)
	if !ok || !isSQLiteDriver(typeOf.PkgPath()) {
		return false
	}

	return methodCodeMatches(original) || valueCodeMatches(value, typeOf)
}

func isSQLiteDriver(packagePath string) bool {
	switch packagePath {
	case "github.com/mattn/go-sqlite3", "github.com/ncruces/go-sqlite3", "modernc.org/sqlite":
		return true
	default:
		return false
	}
}

func indirectValue(value reflect.Value) (reflect.Value, reflect.Type, bool) {
	typeOf := value.Type()
	if typeOf.Kind() != reflect.Pointer {
		return value, typeOf, true
	}

	if value.IsNil() {
		return reflect.Value{}, nil, false
	}

	return value.Elem(), typeOf.Elem(), true
}

func methodCodeMatches(value reflect.Value) bool {
	method := value.MethodByName("Code")
	if !method.IsValid() || method.Type().NumIn() != 0 || method.Type().NumOut() != 1 {
		return false
	}

	return codeValueMatches(method.Call(nil)[0])
}

func valueCodeMatches(value reflect.Value, typeOf reflect.Type) bool {
	if typeOf.Name() == "ErrorCode" || typeOf.Name() == "ExtendedErrorCode" {
		return codeValueMatches(value)
	}

	if value.Kind() != reflect.Struct {
		return false
	}

	return codeValueMatches(value.FieldByName("Code")) ||
		codeValueMatches(value.FieldByName("ExtendedCode"))
}

func codeValueMatches(value reflect.Value) bool {
	switch {
	case !value.IsValid():
		return false
	case value.CanInt():
		code := value.Int()

		return code >= 0 && uint64(code)&primaryCodeMask == busyCode
	case value.CanUint():
		return value.Uint()&primaryCodeMask == busyCode
	default:
		return false
	}
}
