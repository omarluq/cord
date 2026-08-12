package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	t.Parallel()

	result, err := run(context.Background(), 4)

	require.NoError(t, err)
	assert.Equal(t, "result: 10", result)
}
