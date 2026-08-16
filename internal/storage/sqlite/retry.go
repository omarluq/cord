package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/omarluq/cord/internal/backoff"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	primaryCodeMask = 0xff
	retryAttempts   = 20
)

func retryContention(ctx context.Context, operation string, operationFunc func() error) error {
	return retry(ctx, operation, isContention, operationFunc)
}

func retry(
	ctx context.Context,
	operation string,
	retryable func(error) bool,
	operationFunc func() error,
) error {
	const (
		baseDelay = 10 * time.Millisecond
		maxDelay  = 100 * time.Millisecond
	)

	for attempt := 1; attempt <= retryAttempts; attempt++ {
		err := operationFunc()
		if err == nil || !retryable(err) || attempt == retryAttempts {
			return err
		}

		delay := backoff.FullJitter(baseDelay, maxDelay, attempt)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return fmt.Errorf("%s: %w", operation, ctx.Err())
		case <-timer.C:
		}
	}

	return nil
}

func isContention(err error) bool {
	const busyCode = uint64(sqlite3.SQLITE_BUSY)

	if hasDriverCode(err, "github.com/mattn/go-sqlite3", busyCode) ||
		hasDriverCode(err, "github.com/ncruces/go-sqlite3", busyCode) {
		return true
	}

	if sqliteErr, ok := errors.AsType[*sqlite.Error](err); ok {
		return sqliteErr.Code()&primaryCodeMask == sqlite3.SQLITE_BUSY
	}

	return false
}

// hasDriverCode avoids linking optional drivers into Cord. Their error Code
// methods and fields use driver-specific named integer types, so a Go interface
// cannot express the otherwise common contract.
func hasDriverCode(err error, packagePath string, code uint64) bool {
	if err == nil {
		return false
	}

	if driverCodeMatches(err, packagePath, code) {
		return true
	}

	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, nested := range joined.Unwrap() {
			if hasDriverCode(nested, packagePath, code) {
				return true
			}
		}

		return false
	}

	return hasDriverCode(errors.Unwrap(err), packagePath, code)
}

func driverCodeMatches(err error, packagePath string, code uint64) bool {
	original := reflect.ValueOf(err)

	value, typeOf, ok := indirectValue(original)
	if !ok || typeOf.PkgPath() != packagePath {
		return false
	}

	return methodCodeMatches(original, code) || valueCodeMatches(value, typeOf, code)
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

func methodCodeMatches(value reflect.Value, code uint64) bool {
	method := value.MethodByName("Code")
	if !method.IsValid() {
		return false
	}

	results := method.Call(nil)

	return len(results) == 1 && results[0].CanUint() && results[0].Uint()&primaryCodeMask == code
}

func valueCodeMatches(value reflect.Value, typeOf reflect.Type, code uint64) bool {
	if (typeOf.Name() == "ErrorCode" || typeOf.Name() == "ExtendedErrorCode") && value.CanUint() {
		return value.Uint()&primaryCodeMask == code
	}

	if value.Kind() != reflect.Struct {
		return false
	}

	codeField := value.FieldByName("Code")
	if !codeField.IsValid() || !codeField.CanInt() || codeField.Int() < 0 {
		return false
	}

	return codeField.Int()&int64(primaryCodeMask) == int64(sqlite3.SQLITE_BUSY) &&
		code == uint64(sqlite3.SQLITE_BUSY)
}
