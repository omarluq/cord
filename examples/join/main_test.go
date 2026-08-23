package join_test

import (
	"context"
	"testing"

	"github.com/omarluq/cord/examples/join"
	"github.com/omarluq/cord/internal/exampledb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	t.Parallel()

	database := exampledb.DB()

	t.Cleanup(func() { require.NoError(t, database.Close()) })

	result, err := join.Run(context.Background(), database, 4)

	require.NoError(t, err)
	assert.Equal(t, 15, result)
}
