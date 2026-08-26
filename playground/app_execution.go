package playground

import (
	"context"
	"net/url"

	goapp "github.com/maxence-charriere/go-app/v11/pkg/app"
	"github.com/omarluq/cord/playground/internal/protocol"
)

func (app *App) selectExample(filename string) func(goapp.Context, goapp.Event) {
	return func(_ goapp.Context, _ goapp.Event) {
		if !app.mounted || app.status == statusCompiling || app.status == statusRunning {
			return
		}

		source, ok := exampleSource(filename)
		if !ok {
			return
		}

		app.selectedExample = filename
		app.bridge.setSource(source)
		app.bridge.setGraph(protocol.Graph{
			Nodes: []protocol.Node{},
			Edges: []protocol.Edge{},
		})
		app.output = ""
		app.status = statusReady
	}
}

func (app *App) runWorkflow(ctx *goapp.Context, _ goapp.Event) {
	if !app.mounted {
		return
	}

	app.cancelCompilation()
	generation := app.nextGeneration()
	app.status = statusCompiling
	app.output = compilingMessage
	source := app.bridge.source()

	if artifact, ok := app.compilations.get(source); ok {
		app.execute(ctx, artifact, generation)

		return
	}

	compileContext, cancel := context.WithTimeout(
		context.Background(),
		compilationRequestTimeout,
	)
	app.compilationCancel = cancel

	ctx.Async(func() {
		defer cancel()

		artifact, err := compile(
			compileContext,
			app.compilerURL,
			source,
		)

		ctx.Dispatch(func(goapp.Context) {
			app.completeCompilation(generation, source, artifact, err, func() {
				app.execute(ctx, artifact, generation)
			})
		})
	})
}

func (app *App) completeCompilation(
	generation uint64,
	source string,
	artifact compilationArtifact,
	err error,
	execute func(),
) {
	if !app.callbackIsCurrent(generation, true) {
		return
	}

	app.compilationCancel = nil
	if err != nil {
		app.fail(err)

		return
	}

	app.compilations.put(source, artifact)
	execute()
}

func (app *App) execute(ctx *goapp.Context, artifact compilationArtifact, generation uint64) {
	app.status = statusRunning
	app.output = runningMessage
	app.bridge.setGraph(artifact.graph)
	app.bridge.setGraphState("queued")
	app.bridge.runWasm(
		artifact.wasm,
		ctx.Page().URL().ResolveReference(&url.URL{Path: "wasm_exec.js"}).String(),
		func(message *workerEvent) {
			ctx.Dispatch(func(goapp.Context) {
				if app.callbackIsCurrent(generation, true) {
					app.handleWorkerEvent(message)
				}
			})
		},
		func() {
			ctx.Dispatch(func(goapp.Context) {
				if app.callbackIsCurrent(generation, true) && app.status == statusRunning {
					app.status = statusReady
				}
			})
		},
	)
}

func (app *App) handleWorkerEvent(message *workerEvent) {
	switch message.Type() {
	case "node":
		app.bridge.setNodeState(message.ID(), message.State())
	case "output":
		app.output = appendOutput(app.output, message.Value())
	case "error":
		app.bridge.setGraphState("failed")
		app.status = statusFailed
		app.output = appendOutput(app.output, message.Message())
	}
}

func (app *App) fail(err error) {
	if app.mounted {
		app.bridge.setGraphState("failed")
	}

	app.status = statusFailed
	app.output = err.Error()
}
