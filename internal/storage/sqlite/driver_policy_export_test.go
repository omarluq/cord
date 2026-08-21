package sqlite

// MigrationLockPolicyForTest exposes the migration policy type to external tests.
type MigrationLockPolicyForTest = migrationLockPolicy

// MigrationPolicyForTest exposes migration policy selection to external tests.
func MigrationPolicyForTest(driver any) migrationLockPolicy {
	return migrationPolicy(driver)
}

// MigrationPolicyForPackageForTest exposes package-based policy selection to external tests.
func MigrationPolicyForPackageForTest(packagePath string) migrationLockPolicy {
	return migrationPolicyForPackage(packagePath)
}

// LocalMigrationPolicyForTest returns the local migration policy for external tests.
func LocalMigrationPolicyForTest() migrationLockPolicy {
	return migrationLockLocal
}

// RemoteMigrationPolicyForTest returns the remote migration policy for external tests.
func RemoteMigrationPolicyForTest() migrationLockPolicy {
	return migrationLockRemote
}

// SQLiteAffinityForTest exposes SQLite affinity classification to external tests.
func SQLiteAffinityForTest(declaredType string) string {
	return sqliteAffinity(declaredType)
}
