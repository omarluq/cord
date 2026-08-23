package postgres_test

import (
	"testing"

	postgresstore "github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRejectsNilDatabase(t *testing.T) {
	t.Parallel()

	store, err := postgresstore.New(nil)
	require.Error(t, err)
	assert.Nil(t, store)
	assert.EqualError(t, err, "create postgres store: database is nil")
}
