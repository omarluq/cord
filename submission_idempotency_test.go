package cord_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowConcurrentKeyedSubmissionsConflictDeterministically(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-concurrent-conflict", addOne)

	const callers = 24

	results := make(chan asyncSubmitResult, callers)
	start := make(chan struct{})

	var wait sync.WaitGroup
	wait.Add(callers)

	for index := range callers {
		go func() {
			defer wait.Done()

			input := index%2 + 1

			<-start

			id, err := flow.Submit(t.Context(), input, "contended-request")
			results <- asyncSubmitResult{id: id, err: err, input: input}
		}()
	}

	close(start)
	wait.Wait()
	close(results)

	winnerInput := 0

	var winnerID cord.RunID

	conflicts := make([]asyncSubmitResult, 0, callers/2)

	for result := range results {
		if result.err != nil {
			require.ErrorIs(t, result.err, cord.ErrRunConflict)
			conflicts = append(conflicts, result)

			continue
		}

		if winnerInput == 0 {
			winnerInput, winnerID = result.input, result.id
		}

		assert.Equal(t, winnerInput, result.input)
		assert.Equal(t, winnerID, result.id)
	}

	require.NotZero(t, winnerInput)
	require.NotEmpty(t, conflicts)

	for _, conflict := range conflicts {
		assert.NotEqual(t, winnerInput, conflict.input)
	}

	value, err := flow.Get(t.Context(), winnerID)
	require.NoError(t, err)
	assert.Equal(t, winnerInput+1, value)
}

func TestWorkflowIdempotencyScopesDefinitionAndName(t *testing.T) {
	t.Parallel()

	runtime := mustRuntime(t)
	original := runtime.From("async-definition-scope", addOne)
	originalID, err := original.Submit(t.Context(), 3, "same-key")
	require.NoError(t, err)

	changed := runtime.From("async-definition-scope", timesTwo)
	_, err = changed.Submit(t.Context(), 3, "same-key")
	require.ErrorIs(t, err, cord.ErrRunConflict)

	otherName := runtime.From("async-name-scope", addOne)
	otherID, err := otherName.Submit(t.Context(), 3, "same-key")
	require.NoError(t, err)
	assert.NotEqual(t, originalID, otherID)

	originalResult, err := original.Get(t.Context(), originalID)
	require.NoError(t, err)
	assert.Equal(t, 4, originalResult)
	otherResult, err := otherName.Get(t.Context(), otherID)
	require.NoError(t, err)
	assert.Equal(t, 4, otherResult)
}

func TestWorkflowSubmitValidatesIdempotencyKey(t *testing.T) {
	t.Parallel()

	flow := mustRuntime(t).From("async-key-validation", addOne)
	tests := []struct {
		name string
		keys []string
	}{
		{name: "empty", keys: []string{""}},
		{name: "invalid UTF-8", keys: []string{string([]byte{0xff})}},
		{name: "NUL", keys: []string{"a\x00b"}},
		{name: "overlength", keys: []string{strings.Repeat("x", 256)}},
		{name: "multiple", keys: []string{"one", "two"}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runID, err := flow.Submit(t.Context(), 1, testCase.keys...)
			assert.Empty(t, runID)
			require.Error(t, err)
		})
	}
}
