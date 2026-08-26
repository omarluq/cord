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

	tests := []struct {
		name  string
		input int
		want  int
	}{
		{name: "positive", input: 4, want: 15},
		{name: "zero", input: 0, want: 3},
		{name: "negative", input: -2, want: -3},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result, err := join.Run(context.Background(), database, testCase.input)

			require.NoError(t, err)
			assert.Equal(t, testCase.want, result)
			require.NoError(t, database.PingContext(t.Context()), "Run must not close caller-owned database")
		})
	}
}
