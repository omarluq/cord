package playground

import (
	"context"

	goapp "github.com/maxence-charriere/go-app/v11/pkg/app"
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
	app.mount(&ctx)
}

func (app *App) run(ctx goapp.Context, event goapp.Event) {
	app.runWorkflow(&ctx, event)
}

func (app *App) stop(_ goapp.Context, _ goapp.Event) {
	app.nextGeneration()
	app.cancelCompilation()
	app.bridge.stopWasm()
	app.bridge.setGraphState("queued")
	app.status = statusReady
	app.output = "Execution stopped."
}
