package storage

// SQLiteSessionLocker exposes the SQLite migration locker to external tests.
type SQLiteSessionLocker = sqliteSessionLocker

// WrapSQLiteRollbackError exposes rollback error wrapping to external tests.
func WrapSQLiteRollbackError(err error) error {
	return wrapSQLiteRollbackError(err)
}
