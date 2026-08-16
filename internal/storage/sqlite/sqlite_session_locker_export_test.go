package sqlite

// SessionLocker exposes the SQLite migration locker to external tests.
type SessionLocker = sessionLocker

// WrapRollbackError exposes rollback error wrapping to external tests.
func WrapRollbackError(err error) error {
	return wrapRollbackError(err)
}
