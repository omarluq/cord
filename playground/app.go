package playground

import (
	"net/url"
	"strings"

	goapp "github.com/maxence-charriere/go-app/v10/pkg/app"
	"github.com/omarluq/cord/playground/internal/protocol"
)

const (
	defaultCompilerURL = "http://127.0.0.1:4180/compile"
	defaultExample     = "linear.go"
	editorID           = "workflow-editor"
	graphID            = "workflow-graph"
)

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
	bridge browserBridge
	goapp.Compo
	status          appStatus
	output          string
	compilerURL     string
	selectedExample string
	compilations    compilationCache
	mounted         bool
}

// NewApp creates the browser playground component.
func NewApp() *App {
	return &App{
		Compo:           goapp.Compo{},
		bridge:          browserBridge{},
		status:          statusLoading,
		output:          "",
		compilerURL:     defaultCompilerURL,
		mounted:         false,
		selectedExample: defaultExample,
		compilations:    compilationCache{},
	}
}

// OnMount loads the editor and graph modules.
func (app *App) OnMount(ctx goapp.Context) {
	pageURL := ctx.Page().URL()
	if endpoint, ok := compilerEndpoint(
		pageURL,
		pageURL.Query().Get("compiler"),
	); ok {
		app.compilerURL = endpoint
	}

	ctx.Async(func() {
		loadBridge(func(bridge browserBridge, err error) {
			ctx.Dispatch(func(goapp.Context) {
				if err != nil {
					app.fail(err)

					return
				}

				app.bridge = bridge
				app.status = statusReady

				ctx.Defer(func(ctx goapp.Context) {
					app.bridge.mountEditor(editorID, linearSource)
					app.bridge.mountGraph(graphID)
					app.mounted = true

					ctx.Update()
				})
			})
		})
	})
}

// OnDismount stops the currently executing workflow module.
func (app *App) OnDismount() {
	if app.mounted {
		app.bridge.stopWasm()
	}
}

func (app *App) selectExample(_ goapp.Context, event goapp.Event) {
	if !app.mounted {
		return
	}

	filename := event.Get("target").Get("value").String()

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

func (app *App) run(ctx goapp.Context, _ goapp.Event) {
	if !app.mounted {
		return
	}

	app.status = statusCompiling
	app.output = compilingMessage
	source := app.bridge.source()

	if artifact, ok := app.compilations.get(source); ok {
		app.execute(ctx, artifact)

		return
	}

	ctx.Async(func() {
		artifact, err := compile(ctx, app.compilerURL, source)
		ctx.Dispatch(func(goapp.Context) {
			if err != nil {
				app.fail(err)

				return
			}

			app.compilations.put(source, artifact)
			app.execute(ctx, artifact)
		})
	})
}

func (app *App) execute(ctx goapp.Context, artifact compilationArtifact) {
	app.status = statusRunning
	app.output = runningMessage
	app.bridge.setGraph(artifact.graph)
	app.bridge.setGraphState("queued")
	app.bridge.runWasm(
		artifact.wasm,
		ctx.Page().URL().ResolveReference(&url.URL{Path: "wasm_exec.js"}).String(),
		func(message *workerEvent) {
			ctx.Dispatch(func(goapp.Context) {
				app.handleWorkerEvent(message)
			})
		},
		func() {
			ctx.Dispatch(func(goapp.Context) {
				if app.status == statusRunning {
					app.status = statusReady
				}
			})
		},
	)
}

func (app *App) stop(_ goapp.Context, _ goapp.Event) {
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
	case "exit":
	}
}

func (app *App) fail(err error) {
	if app.mounted {
		app.bridge.setGraphState("failed")
	}

	app.status = statusFailed
	app.output = err.Error()
}

func compilerEndpoint(pageURL *url.URL, endpoint string) (string, bool) {
	if endpoint == "" {
		return "", false
	}

	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.User != nil {
		return "", false
	}

	resolved := pageURL.ResolveReference(parsed)
	if resolved.Scheme == pageURL.Scheme && resolved.Host == pageURL.Host {
		return resolved.String(), true
	}

	// Local development serves the static app and compiler on separate ports.
	if isLoopbackHost(pageURL.Hostname()) && endpoint == defaultCompilerURL {
		return endpoint, true
	}

	return "", false
}

func isLoopbackHost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1" ||
		strings.HasSuffix(host, ".localhost")
}
