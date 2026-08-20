package sqlite

func MigrationPolicyForTest(driver any) migrationLockPolicy {
	return migrationPolicy(driver)
}

func MigrationPolicyForPackageForTest(packagePath string) migrationLockPolicy {
	return migrationPolicyForPackage(packagePath)
}

func LocalMigrationPolicyForTest() migrationLockPolicy {
	return migrationLockLocal
}

func RemoteMigrationPolicyForTest() migrationLockPolicy {
	return migrationLockRemote
}
