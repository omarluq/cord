package playground

import (
	"context"
	"net/url"
	"strings"

	goapp "github.com/maxence-charriere/go-app/v11/pkg/app"
	"github.com/omarluq/cord/playground/internal/protocol"
)

const (
	defaultCompilerURL = "http://127.0.0.1:4180/compile"
	defaultExample     = "linear.go"
	editorID           = "workflow-editor"
	graphID            = "workflow-graph"
)

// configuredCompilerURL is set at build time for cross-origin deployments.
var configuredCompilerURL string

type appStatus string

const (
	statusLoading   appStatus = "loading"
	statusReady     appStatus = "ready"
	statusCompiling appStatus = "compiling"
	statusRunning   appStatus = "running"
	statusFailed    appStatus = "failed"
)

// App is the browser playground component.
type App struct {
	bridge            browserBridge
	compilationCancel context.CancelFunc
	goapp.Compo
	status          appStatus
	output          string
	compilerURL     string
	selectedExample string
	compilations    compilationCache
	generation      uint64
	active          bool
	mounted         bool
}

// NewApp creates the browser playground component.
func NewApp() *App {
	return &App{
		Compo:             goapp.Compo{},
		bridge:            browserBridge{},
		status:            statusLoading,
		output:            "",
		compilerURL:       defaultCompilerURL,
		active:            false,
		mounted:           false,
		selectedExample:   defaultExample,
		compilations:      compilationCache{},
		compilationCancel: nil,
		generation:        0,
	}
}

// OnMount loads the editor and graph modules.
func (app *App) OnMount(ctx goapp.Context) {
	app.cancelCompilation()
	app.active = true
	generation := app.nextGeneration()

	pageURL := ctx.Page().URL()
	if endpoint, ok := compilerEndpoint(
		pageURL,
		pageURL.Query().Get("compiler"),
		configuredCompilerURL,
	); ok {
		app.compilerURL = endpoint
	}

	ctx.Async(func() {
		loadBridge(func(bridge browserBridge, err error) {
			ctx.Dispatch(func(goapp.Context) {
				if !app.callbackIsCurrent(generation, false) {
					return
				}

				if err != nil {
					app.fail(err)

					return
				}

				app.bridge = bridge
				app.status = statusReady

				ctx.Defer(func(ctx goapp.Context) {
					if !app.callbackIsCurrent(generation, false) {
						return
					}

					app.bridge.mountEditor(editorID, linearSource)
					app.bridge.mountGraph(graphID)
					app.mounted = true

					ctx.Update()
				})
			})
		})
	})
}

// OnDismount cancels compilation and stops the executing workflow module.
func (app *App) OnDismount() {
	app.active = false
	app.nextGeneration()
	app.cancelCompilation()

	if app.mounted {
		app.bridge.destroy()
		app.mounted = false
	}
}

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

func (app *App) run(ctx goapp.Context, _ goapp.Event) {
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

func (app *App) execute(ctx goapp.Context, artifact compilationArtifact, generation uint64) {
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

func (app *App) stop(_ goapp.Context, _ goapp.Event) {
	app.nextGeneration()
	app.cancelCompilation()
	app.bridge.stopWasm()
	app.bridge.setGraphState("queued")
	app.status = statusReady
	app.output = "Execution stopped."
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

func (app *App) nextGeneration() uint64 {
	app.generation++

	return app.generation
}

func (app *App) callbackIsCurrent(generation uint64, requireMounted bool) bool {
	return app.active && app.generation == generation && (!requireMounted || app.mounted)
}

func (app *App) cancelCompilation() {
	if app.compilationCancel == nil {
		return
	}

	app.compilationCancel()
	app.compilationCancel = nil
}

func compilerEndpoint(pageURL *url.URL, endpoint, configuredEndpoint string) (string, bool) {
	if endpoint == "" {
		endpoint = configuredEndpoint
	}

	parsed, ok := parseCompilerEndpoint(endpoint)
	if !ok {
		return "", false
	}

	resolved := pageURL.ResolveReference(parsed)
	if sameOrigin(pageURL, resolved) {
		return resolved.String(), true
	}

	if endpoint == configuredEndpoint && parsed.IsAbs() && parsed.Scheme == "https" {
		return parsed.String(), true
	}

	// Local development serves the static app and compiler on separate ports.
	if isLoopbackHost(pageURL.Hostname()) && endpoint == defaultCompilerURL {
		return endpoint, true
	}

	return "", false
}

func parseCompilerEndpoint(endpoint string) (*url.URL, bool) {
	if endpoint == "" {
		return nil, false
	}

	parsed, err := url.Parse(endpoint)

	return parsed, err == nil && parsed.User == nil
}

func sameOrigin(first, second *url.URL) bool {
	return first.Scheme == second.Scheme && first.Host == second.Host
}

func isLoopbackHost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1" ||
		strings.HasSuffix(host, ".localhost")
}
