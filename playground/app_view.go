package playground

import (
	goapp "github.com/maxence-charriere/go-app/v10/pkg/app"
)

var (
	panelClasses = []string{
		"overflow-hidden",
		"border",
		"border-nord-3",
		"bg-nord-1",
	}
	buttonClasses = []string{
		"cursor-pointer",
		"rounded-md",
		"border",
		"border-nord-3",
		"bg-nord-2",
		"px-3",
		"py-2",
		"text-xs",
		"font-semibold",
		"transition-colors",
		"hover:not-disabled:border-nord-8",
		"disabled:cursor-not-allowed",
		"disabled:opacity-40",
	}
)

// Render builds the editor, graph, and output interface.
func (app *App) Render() goapp.UI {
	return goapp.Main().Class(
		"flex",
		"h-dvh",
		"min-h-0",
		"flex-col",
		"overflow-hidden",
	).Body(
		goapp.Div().Class(
			"grid",
			"min-h-0",
			"w-full",
			"flex-1",
			"grid-cols-2",
			"overflow-hidden",
		).Body(
			app.renderEditorPanel(),
			app.renderResultsPanel(),
		),
	)
}

func (app *App) renderEditorPanel() goapp.UI {
	return goapp.Section().Class(append(
		panelClasses,
		"flex",
		"min-h-0",
		"flex-col",
	)...).Body(
		goapp.Div().Class(
			"flex",
			"shrink-0",
			"items-center",
			"justify-between",
			"gap-4",
			"border-b",
			"border-nord-3",
			"px-4",
			"py-3",
		).Body(
			app.renderExampleSelect(),
			goapp.Div().Class("flex", "items-center", "gap-3").Body(
				goapp.Span().
					DataSet("testid", "status").
					Class("sr-only").
					Aria("live", "polite").
					Text(string(app.status)),
				app.renderExecutionButton(),
			),
		),
		goapp.Div().ID(editorID).Class("min-h-0", "flex-1"),
	)
}

func (app *App) renderResultsPanel() goapp.UI {
	return goapp.Div().Class(
		"grid",
		"min-h-0",
		"min-w-0",
		"grid-rows-[minmax(0,2fr)_minmax(0,1fr)]",
		"overflow-hidden",
	).Body(
		app.renderGraphPanel(),
		goapp.Section().Class(append(
			panelClasses,
			"flex",
			"min-h-0",
			"flex-col",
		)...).Body(
			goapp.Div().Class(
				"shrink-0",
				"border-b",
				"border-nord-3",
				"px-4",
				"py-3",
			).Body(
				goapp.H2().Class(
					"text-xs",
					"font-bold",
					"uppercase",
					"tracking-wider",
				).Text("Output"),
			),
			goapp.Pre().
				DataSet("testid", "run-result").
				Class(
					"min-h-0",
					"flex-1",
					"overflow-x-hidden",
					"overflow-y-auto",
					"whitespace-pre-wrap",
					"p-4",
					"font-mono",
					"text-sm",
					"text-nord-14",
				).
				Text(app.output),
		),
	)
}

func (app *App) renderGraphPanel() goapp.UI {
	return goapp.Section().Class(append(
		panelClasses,
		"relative",
		"flex",
		"min-h-0",
		"flex-col",
	)...).Body(
		goapp.Div().Class(
			"absolute",
			"top-3",
			"right-3",
			"z-10",
			"flex",
			"gap-1",
		).Body(
			app.renderZoomButton("out", -1),
			app.renderZoomButton("in", 1),
		),
		goapp.Div().ID(graphID).Class(
			"min-h-0",
			"flex-1",
			"bg-nord-0",
		),
	)
}

func (app *App) renderZoomButton(direction string, amount int) goapp.UI {
	label := "Zoom " + direction + " workflow graph"
	return goapp.Button().
		Aria("label", label).
		Title("Zoom "+direction).
		Class(
			"graph-zoom",
			"graph-zoom-"+direction,
			"relative",
			"size-8",
			"cursor-pointer",
			"rounded",
			"border",
			"border-nord-3",
			"bg-nord-1/90",
			"text-nord-5",
			"hover:border-nord-8",
			"hover:text-nord-8",
		).
		OnClick(func(goapp.Context, goapp.Event) {
			app.bridge.zoomGraph(amount)
		})
}

func (app *App) renderExecutionButton() goapp.UI {
	classes := append(
		buttonClasses,
		"execution-toggle",
		"size-9",
		"p-0",
	)
	if app.status == statusRunning {
		return goapp.Button().
			Aria("label", "Stop workflow").
			Title("Stop workflow").
			Class(append(
				classes,
				"execution-toggle-stop",
				"border-nord-11",
				"text-nord-11",
			)...).
			OnClick(app.stop)
	}

	return goapp.Button().
		Aria("label", "Compile and run workflow").
		Title("Compile and run workflow").
		Class(append(
			classes,
			"execution-toggle-start",
			"border-nord-10",
			"bg-nord-10",
			"text-white",
		)...).
		Disabled(
			app.status != statusReady &&
				app.status != statusFailed,
		).
		OnClick(app.run)
}

func (app *App) renderExampleSelect() goapp.UI {
	options := make([]goapp.UI, 0, len(exampleScripts))
	for _, script := range exampleScripts {
		options = append(
			options,
			goapp.Option().
				Value(script.filename).
				Selected(script.filename == app.selectedExample).
				Text(script.filename),
		)
	}

	return goapp.Div().Class("relative").Body(
		goapp.Select().
			Aria("label", "Example workflow").
			Class(
				"cursor-pointer",
				"appearance-none",
				"rounded",
				"border",
				"border-nord-3",
				"bg-nord-0",
				"py-1",
				"pr-2",
				"pl-8",
				"font-mono",
				"text-xs",
				"text-nord-5",
			).
			Disabled(
				app.status == statusCompiling ||
					app.status == statusRunning,
			).
			OnChange(app.selectExample).
			Body(options...),
		goapp.Img().
			Src("/web/go-logo.svg").
			Alt("").
			Aria("hidden", "true").
			Class(
				"pointer-events-none",
				"absolute",
				"top-1/2",
				"left-2",
				"h-4",
				"w-4",
				"-translate-y-1/2",
				"object-contain",
			),
	)
}
