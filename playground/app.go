package playground

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	goapp "github.com/maxence-charriere/go-app/v10/pkg/app"
)

const (
	defaultCompilerURL = "/compile"
	editorID           = "workflow-editor"
	graphID            = "workflow-graph"
	panelClasses       = "overflow-hidden rounded-md border border-nord-3 bg-nord-1"
	buttonClasses      = "cursor-pointer rounded-md border border-nord-3 bg-nord-2 px-3 py-2 text-xs font-semibold transition-colors hover:not-disabled:border-nord-8 disabled:cursor-not-allowed disabled:opacity-40"
)

const defaultSource = `package main

import (
    "context"
    "fmt"

    "github.com/omarluq/cord/playground/client"
)

func increment(_ context.Context, value int) (int, error) {
    return client.Step("increment", func() (int, error) {
        return value + 1, nil
    })
}

func double(_ context.Context, value int) (int, error) {
    return client.Step("double", func() (int, error) {
        return value * 2, nil
    })
}

func main() {
    ctx := context.Background()
    session, err := client.NewSession(ctx)
    if err != nil {
        client.EmitError(err)
        return
    }
    defer session.Close()

    client.Graph(
        []client.Node{{ID: "increment", Label: "Increment"}, {ID: "double", Label: "Double"}},
        []client.Edge{{From: "increment", To: "double"}},
    )

    result, err := session.Cord.From("playground", increment).Then(double).Run(ctx, 4)
    if err != nil {
        client.EmitError(err)
        return
    }
    client.Result(fmt.Sprintf("result: %d", result))
}
`

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
	goapp.Compo
	bridge      browserBridge
	status      appStatus
	output      string
	compilerURL string
	mounted     bool
}

// NewApp creates the browser playground component.
func NewApp() *App {
	return &App{
		Compo:       goapp.Compo{},
		bridge:      browserBridge{},
		status:      statusLoading,
		output:      "",
		compilerURL: defaultCompilerURL,
		mounted:     false,
	}
}

// OnMount loads the editor and graph modules.
func (app *App) OnMount(ctx goapp.Context) {
	if endpoint := ctx.Page().URL().Query().Get("compiler"); endpoint != "" {
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
				ctx.Defer(func(goapp.Context) {
					app.bridge.mountEditor(editorID, defaultSource)
					app.bridge.mountGraph(graphID)
					app.mounted = true
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

// Render builds the editor, graph, and output interface.
func (app *App) Render() goapp.UI {
	button := utilityClasses(buttonClasses)

	return goapp.Main().Class("min-h-screen").Body(
		goapp.Header().Class(utilityClasses("flex items-center justify-between gap-4 border-b border-nord-3 px-[clamp(1rem,4vw,3rem)] py-4")...).Body(
			goapp.Div().Body(
				goapp.H1().Class("text-lg", "font-bold", "text-nord-6").Text("Cord Playground"),
				goapp.P().Class("text-xs", "text-[#9aa6b6]").Text("Write Go. Compile to WebAssembly. Run Cord in your browser."),
			),
			goapp.Div().Class("flex", "items-center", "gap-3").Body(
				goapp.Span().DataSet("testid", "status").Class("font-mono", "text-xs", "uppercase", "text-nord-8").Text(string(app.status)),
				goapp.Button().Class(append(button, "border-nord-10", "bg-nord-10", "text-white")...).Disabled(app.status != statusReady && app.status != statusFailed).OnClick(app.run).Text("Compile & run"),
				goapp.Button().Class(button...).Disabled(app.status != statusRunning).OnClick(app.stop).Text("Stop"),
			),
		),
		goapp.Div().Class(utilityClasses("mx-auto grid max-w-[1600px] grid-cols-2 gap-4 p-[clamp(1rem,3vw,2rem)] max-[900px]:grid-cols-1")...).Body(
			goapp.Section().Class(utilityClasses(panelClasses)...).Body(
				goapp.Div().Class("border-b", "border-nord-3", "px-4", "py-3").Body(goapp.H2().Class("text-xs", "font-bold", "uppercase", "tracking-wider").Text("Workflow source")),
				goapp.Div().ID(editorID).Class("h-[68vh]", "min-h-[32rem]"),
			),
			goapp.Div().Class("grid", "min-w-0", "grid-rows-[minmax(22rem,1fr)_auto]", "gap-4").Body(
				goapp.Section().Class(utilityClasses(panelClasses)...).Body(
					goapp.Div().Class("border-b", "border-nord-3", "px-4", "py-3").Body(goapp.H2().Class("text-xs", "font-bold", "uppercase", "tracking-wider").Text("Workflow DAG")),
					goapp.Div().ID(graphID).Class("h-full", "min-h-[22rem]", "bg-nord-0"),
				),
				goapp.Section().Class(utilityClasses(panelClasses)...).Body(
					goapp.Div().Class("border-b", "border-nord-3", "px-4", "py-3").Body(goapp.H2().Class("text-xs", "font-bold", "uppercase", "tracking-wider").Text("Output")),
					goapp.Pre().DataSet("testid", "run-result").Class("min-h-24", "overflow-auto", "p-4", "font-mono", "text-sm", "text-nord-14").Text(app.output),
				),
			),
		),
	)
}

func (app *App) run(ctx goapp.Context, _ goapp.Event) {
	if !app.mounted {
		return
	}

	app.status = statusCompiling
	app.output = "Compiling workflow…"
	source := app.bridge.source()
	endpoint := app.compilerURL

	ctx.Async(func() {
		wasm, err := compile(ctx, endpoint, source)
		ctx.Dispatch(func(goapp.Context) {
			if err != nil {
				app.fail(err)
				return
			}

			app.status = statusRunning
			app.output = "Running workflow…"
			app.bridge.runWasm(wasm, ctx.Page().URL().ResolveReference(&url.URL{Path: "wasm_exec.js"}).String(), func(message workerEvent) {
				ctx.Dispatch(func(goapp.Context) { app.handleWorkerEvent(message) })
			}, func() {
				ctx.Dispatch(func(goapp.Context) {
					if app.status == statusRunning {
						app.status = statusReady
					}
				})
			})
		})
	})
}

func (app *App) stop(_ goapp.Context, _ goapp.Event) {
	app.bridge.stopWasm()
	app.status = statusReady
	app.output = "Execution stopped."
}

func (app *App) handleWorkerEvent(message workerEvent) {
	switch message.Type() {
	case "graph":
		app.bridge.setGraph(message)
	case "node":
		app.bridge.setNodeState(message.ID(), message.State())
		if detail := message.Message(); detail != "" {
			app.output = detail
		}
	case "result":
		app.output = message.Value()
	case "error":
		app.status = statusFailed
		app.output = message.Message()
	case "exit":
	}
}

func (app *App) fail(err error) {
	app.status = statusFailed
	app.output = err.Error()
}

func compile(ctx goapp.Context, endpoint, source string) ([]byte, error) {
	body, err := json.Marshal(map[string]string{"source": source})
	if err != nil {
		return nil, fmt.Errorf("encode compilation request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create compilation request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("compile workflow: %w", err)
	}
	result, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("read compilation response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		var failure struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(result, &failure) == nil && failure.Error != "" {
			return nil, fmt.Errorf("compile workflow: %s", failure.Error)
		}
		return nil, fmt.Errorf("compile workflow: %s", response.Status)
	}

	return result, nil
}

func utilityClasses(value string) []string {
	return strings.Fields(value)
}
