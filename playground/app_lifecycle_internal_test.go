package playground

import (
	"context"
	"errors"
	"testing"

	"github.com/omarluq/cord/playground/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppNavigationDismountCancelsCompilationAndIgnoresLateCallbacks(t *testing.T) {
	t.Parallel()

	lateFailure := errors.New("late compilation failure")
	tests := []struct {
		err  error
		name string
	}{
		{name: "late success", err: nil},
		{name: "late failure", err: lateFailure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			app := NewApp()
			app.active = true
			app.mounted = true
			app.status = statusCompiling
			app.output = compilingMessage
			generation := app.nextGeneration()
			requestContext, cancel := context.WithCancel(context.Background())
			app.compilationCancel = cancel

			app.OnDismount()

			require.ErrorIs(t, requestContext.Err(), context.Canceled)
			assert.False(t, app.active)
			assert.False(t, app.mounted)
			assert.Nil(t, app.compilationCancel)

			executed := false
			artifact := compilationArtifact{
				graph: protocol.Graph{},
				wasm:  []byte("late wasm"),
			}
			app.completeCompilation(generation, "late source", artifact, test.err, func() {
				executed = true
			})

			assert.False(t, executed)

			_, cached := app.compilations.get("late source")
			assert.False(t, cached)
			assert.Equal(t, statusCompiling, app.status)
			assert.Equal(t, compilingMessage, app.output)
		})
	}
}

func TestAppAcceptsSubsequentCompilationAfterDismount(t *testing.T) {
	t.Parallel()

	app := NewApp()
	app.active = true
	app.mounted = true
	staleGeneration := app.nextGeneration()
	staleContext, staleCancel := context.WithCancel(context.Background())
	app.compilationCancel = staleCancel

	app.OnDismount()
	require.ErrorIs(t, staleContext.Err(), context.Canceled)

	// Simulate go-app navigating back and mounting this component again.
	app.active = true
	app.mounted = true
	currentGeneration := app.nextGeneration()
	currentContext, currentCancel := context.WithCancel(context.Background())
	app.compilationCancel = currentCancel

	staleExecuted := false

	app.completeCompilation(
		staleGeneration,
		"stale source",
		compilationArtifact{
			graph: protocol.Graph{},
			wasm:  []byte("stale wasm"),
		},
		nil,
		func() { staleExecuted = true },
	)
	assert.False(t, staleExecuted)
	assert.NotNil(t, app.compilationCancel)

	currentExecuted := false
	currentArtifact := compilationArtifact{
		graph: protocol.Graph{},
		wasm:  []byte("current wasm"),
	}
	app.completeCompilation(
		currentGeneration,
		"current source",
		currentArtifact,
		nil,
		func() { currentExecuted = true },
	)

	assert.True(t, currentExecuted)
	assert.Nil(t, app.compilationCancel)
	require.NoError(t, currentContext.Err())

	cached, found := app.compilations.get("current source")
	require.True(t, found)
	assert.Equal(t, currentArtifact, cached)
}

func TestCompilationCallbackGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		generation uint64
		active     bool
		mounted    bool
		guardMount bool
		want       bool
	}{
		{name: "current mounted", generation: 3, active: true, mounted: true, guardMount: true, want: true},
		{name: "dismounted", generation: 3, active: false, mounted: false, guardMount: true, want: false},
		{name: "stale", generation: 2, active: true, mounted: true, guardMount: true, want: false},
		{
			name: "mount callback before bridge mount", generation: 3,
			active: true, mounted: false, guardMount: false, want: true,
		},
		{
			name: "compilation callback before bridge mount", generation: 3,
			active: true, mounted: false, guardMount: true, want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			app := NewApp()
			app.active = test.active
			app.mounted = test.mounted
			app.generation = 3

			assert.Equal(
				t,
				test.want,
				app.callbackIsCurrent(test.generation, test.guardMount),
			)
		})
	}
}
