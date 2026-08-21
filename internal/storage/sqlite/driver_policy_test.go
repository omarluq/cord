package sqlite_test

import (
	"testing"

	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
)

type localTestDriver struct{}

type wrappedTestDriver struct {
	driver any
}

func TestMigrationPolicy(t *testing.T) {
	t.Parallel()

	local := localTestDriver{}
	localPointer := &local
	tests := []struct {
		driver any
		name   string
		want   sqlite.MigrationLockPolicyForTest
	}{
		{driver: nil, name: "nil", want: sqlite.LocalMigrationPolicyForTest()},
		{driver: local, name: "local value", want: sqlite.LocalMigrationPolicyForTest()},
		{driver: &local, name: "local pointer", want: sqlite.LocalMigrationPolicyForTest()},
		{driver: &localPointer, name: "local nested pointer", want: sqlite.LocalMigrationPolicyForTest()},
		{
			driver: wrappedTestDriver{driver: local},
			name:   "wrapped driver retains safe established fallback",
			want:   sqlite.LocalMigrationPolicyForTest(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, sqlite.MigrationPolicyForTest(test.driver))
		})
	}
}

func TestMigrationPolicyForPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		packagePath string
		want        sqlite.MigrationLockPolicyForTest
	}{
		{
			name:        "supported libsql",
			packagePath: "github.com/tursodatabase/go-libsql",
			want:        sqlite.RemoteMigrationPolicyForTest(),
		},
		{
			name: "unknown", packagePath: "example.com/remote-sqlite", want: sqlite.LocalMigrationPolicyForTest(),
		},
		{name: "empty", packagePath: "", want: sqlite.LocalMigrationPolicyForTest()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, sqlite.MigrationPolicyForPackageForTest(test.packagePath))
		})
	}
}
