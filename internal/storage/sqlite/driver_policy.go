package sqlite

import "reflect"

type migrationLockPolicy uint8

const (
	migrationLockLocal migrationLockPolicy = iota
	migrationLockRemote
)

func migrationPolicy(driver any) migrationLockPolicy {
	typeOf := reflect.TypeOf(driver)
	if typeOf == nil {
		return migrationLockLocal
	}

	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}

	return migrationPolicyForPackage(typeOf.PkgPath())
}

func migrationPolicyForPackage(packagePath string) migrationLockPolicy {
	// Remote locking is deliberately limited to the supported libSQL driver.
	// Unknown drivers retain the established local-lock policy.
	if packagePath == "github.com/tursodatabase/go-libsql" {
		return migrationLockRemote
	}

	return migrationLockLocal
}
