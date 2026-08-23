package linear_test

import (
	"context"
	"testing"

	"github.com/omarluq/cord/examples/linear"
	"github.com/omarluq/cord/internal/exampledb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	t.Parallel()

	database := exampledb.DB()

	t.Cleanup(func() { require.NoError(t, database.Close()) })

	result, err := linear.Run(context.Background(), database, 4)

	require.NoError(t, err)
	assert.Equal(t, "result: 10", result)
}
