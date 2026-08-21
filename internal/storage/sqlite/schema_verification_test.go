package sqlite_test

import (
	"testing"

	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestSQLiteAffinity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		declaredType string
		want         string
	}{
		{declaredType: "DOUBLE", want: "REAL"},
		{declaredType: "DOUBLE PRECISION", want: "REAL"},
	}

	for _, test := range tests {
		t.Run(test.declaredType, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, sqlite.SQLiteAffinityForTest(test.declaredType))
		})
	}
}
