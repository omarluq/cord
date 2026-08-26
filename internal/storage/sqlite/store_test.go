package sqlite_test

import (
	"github.com/omarluq/cord/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNew_RejectsNilDatabase(t *testing.T) {
	t.Parallel()

	store, err := sqlite.New(nil)

	assert.Nil(t, store)
	require.Error(t, err)
}
