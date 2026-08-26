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

	tests := []struct {
		name  string
		want  string
		input int
	}{
		{name: "positive", input: 4, want: "result: 10"},
		{name: "zero", input: 0, want: "result: 2"},
		{name: "negative", input: -2, want: "result: -2"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result, err := linear.Run(context.Background(), database, testCase.input)

			require.NoError(t, err)
			assert.Equal(t, testCase.want, result)
			require.NoError(t, database.PingContext(t.Context()), "Run must not close caller-owned database")
		})
	}
}
