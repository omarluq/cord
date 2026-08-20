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
		want   uint8
	}{
		{driver: nil, name: "nil", want: uint8(sqlite.LocalMigrationPolicyForTest())},
		{driver: local, name: "local value", want: uint8(sqlite.LocalMigrationPolicyForTest())},
		{driver: &local, name: "local pointer", want: uint8(sqlite.LocalMigrationPolicyForTest())},
		{driver: &localPointer, name: "local nested pointer", want: uint8(sqlite.LocalMigrationPolicyForTest())},
		{
			driver: wrappedTestDriver{driver: local},
			name:   "wrapped driver retains safe established fallback",
			want:   uint8(sqlite.LocalMigrationPolicyForTest()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, uint8(sqlite.MigrationPolicyForTest(test.driver)))
		})
	}
}

func TestMigrationPolicyForPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		packagePath string
		want        uint8
	}{
		{
			name:        "supported libsql",
			packagePath: "github.com/tursodatabase/go-libsql",
			want:        uint8(sqlite.RemoteMigrationPolicyForTest()),
		},
		{name: "unknown", packagePath: "example.com/remote-sqlite", want: uint8(sqlite.LocalMigrationPolicyForTest())},
		{name: "empty", packagePath: "", want: uint8(sqlite.LocalMigrationPolicyForTest())},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, uint8(sqlite.MigrationPolicyForPackageForTest(test.packagePath)))
		})
	}
}
