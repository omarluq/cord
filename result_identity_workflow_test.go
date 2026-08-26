package cord_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowGetErrors(t *testing.T) {
	t.Parallel()

	runtime := mustRuntime(t)
	flow := runtime.From("async-get-errors", addOne)

	_, err := flow.Get(t.Context(), "missing")
	require.ErrorIs(t, err, cord.ErrRunNotFound)

	_, err = flow.Get(t.Context(), "")
	require.Error(t, err)

	other := runtime.From("async-get-other", addOne)
	id, err := flow.Submit(t.Context(), 1)
	require.NoError(t, err)
	_, err = other.Get(t.Context(), id)
	assert.ErrorIs(t, err, cord.ErrRunIncompatible)
}

func TestWorkflowGetVerifiesCompleteDefinition(t *testing.T) {
	t.Parallel()

	runtime := mustRuntime(t)
	flow := runtime.From("async-complete-definition", addOne)
	runID, err := flow.Submit(t.Context(), 1)
	require.NoError(t, err)

	// This handle has the same workflow name and terminal signature, but a
	// different compiled graph and terminal identity.
	other := runtime.From("async-complete-definition", passThrough).Then(addOne)
	_, err = other.Get(t.Context(), runID)
	assert.ErrorIs(t, err, cord.ErrRunIncompatible)
}

func TestWorkflowGetUsesPersistedRetryPolicy(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	first := newRuntimeForDB(t, database, cord.Options{
		MaxAttempts: 7, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 2 * time.Millisecond,
	})
	flow := first.From("async-persisted-retry", addOne)
	id, err := flow.Submit(t.Context(), 41)
	require.NoError(t, err)

	second := newRuntimeForDB(t, database)
	result, err := second.From("async-persisted-retry", addOne).Get(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}
