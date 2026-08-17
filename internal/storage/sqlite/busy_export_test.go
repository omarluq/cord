package sqlite

// IsBusyForTest exposes busy-error classification to external driver tests.
func IsBusyForTest(err error) bool {
	return isBusy(err)
}
