package playground

import goapp "github.com/maxence-charriere/go-app/v11/pkg/app"

// Render builds the editor, graph, and output interface.
func (app *App) Render() goapp.UI {
	return goapp.Main().Class("flex", "h-dvh", classMinHeight0, classFlexColumn, classOverflowHidden).Body(
		goapp.Div().ID("playground-layout").Class(
			"grid", "grid-cols-[192px_1px_minmax(0,1fr)_1px_minmax(240px,1fr)]",
			classMinHeight0, "w-full", classFlex1, classOverflowHidden,
		).Body(
			app.renderFileTree(),
			app.renderResizeHandle("files", "vertical", "Resize file tree and editor"),
			app.renderEditorPanel(),
			app.renderResizeHandle("results", "vertical", "Resize editor and results"),
			app.renderResultsPanel(),
		),
	)
}

func (app *App) renderEditorPanel() goapp.UI {
	return goapp.Section().DataSet(dataResizePanel, "editor").Class(
		"relative", "flex", classMinHeight0, classMinWidth0, classFlexColumn,
		classOverflowHidden, classBackgroundNord1,
	).Body(
		goapp.Span().DataSet("testid", "status").Class("sr-only").
			Aria("live", "polite").Text(string(app.status)),
		goapp.Div().Class("absolute", "right-3", "top-3", "z-20").Body(app.renderExecutionButton()),
		goapp.Div().ID(editorID).Aria("label", "Workflow Go source").Class(classMinHeight0, classFlex1),
	)
}

func (app *App) renderResultsPanel() goapp.UI {
	return goapp.Div().ID("results-layout").DataSet(dataResizePanel, "results").Class(
		"grid", "grid-rows-[minmax(0,2fr)_1px_minmax(0,1fr)]",
		classMinHeight0, classMinWidth0, classOverflowHidden,
	).Body(
		app.renderGraphPanel(),
		app.renderResizeHandle("output", "horizontal", "Resize graph and output"),
		goapp.Section().DataSet(dataResizePanel, "output").Class(
			"flex", classMinHeight0, classFlexColumn, classOverflowHidden, classBackgroundNord1,
		).Body(
			goapp.Pre().DataSet("testid", "run-result").Class(
				classMinHeight0, classFlex1, "overflow-x-hidden", "overflow-y-auto",
				"whitespace-pre-wrap", "p-4", "font-mono", "text-sm", "text-nord-14",
			).Text(app.output),
		),
	)
}

func (app *App) renderGraphPanel() goapp.UI {
	return goapp.Section().DataSet(dataResizePanel, "graph").Class(
		"relative", "flex", classMinHeight0, classFlexColumn, classOverflowHidden, classBackgroundNord1,
	).Body(
		goapp.Div().Class("absolute", "top-3", "right-3", "z-10", "flex", "gap-1").Body(
			app.renderZoomButton("out", -1), app.renderZoomButton("in", 1),
		),
		goapp.Div().ID(graphID).Class(classMinHeight0, classFlex1, "bg-nord-0"),
	)
}

func (app *App) renderResizeHandle(name, orientation, label string) goapp.UI {
	classes := []string{
		"relative", "z-30", "bg-nord-3", "touch-none", "after:absolute", "after:content-['']",
		"hover:bg-nord-8", "focus-visible:bg-nord-8",
	}
	if orientation == "vertical" {
		classes = append(classes, "cursor-col-resize", "after:inset-y-0", "after:-inset-x-1.5")
	} else {
		classes = append(classes, "cursor-row-resize", "after:-inset-y-1.5", "after:inset-x-0")
	}

	return goapp.Div().Role("separator").Aria("label", label).Aria("orientation", orientation).
		Aria("valuemin", "0").Aria("valuemax", "100").Aria("valuenow", "50").
		DataSet("resize-handle", name).DataSet("testid", name+"-resize-handle").TabIndex(0).Class(classes...)
}

func (app *App) renderZoomButton(direction string, amount int) goapp.UI {
	label := "Zoom " + direction + " workflow graph"

	icon := []goapp.UI{goapp.Span().Class(
		"absolute", "h-0.5", "w-3", "rounded-full", classBackgroundCurrent,
	)}
	if direction == "in" {
		icon = append(icon, goapp.Span().Class(
			"absolute", "h-3", "w-0.5", "rounded-full", classBackgroundCurrent,
		))
	}

	return goapp.Button().Aria("label", label).Title("Zoom "+direction).Disabled(!app.mounted).Class(
		"relative", "flex", "size-8", classCursorPointer, classItemsCenter, "justify-center", "rounded",
		"border", classBorderNord3, "bg-nord-1/90", "text-nord-5", "hover:border-nord-8", "hover:text-nord-8",
	).OnClick(func(goapp.Context, goapp.Event) { app.bridge.zoomGraph(amount) }).Body(icon...)
}

func (app *App) renderExecutionButton() goapp.UI {
	classes := append(buttonClasses(), "flex", "size-9", classItemsCenter, "justify-center", "p-0")
	if app.status == statusRunning {
		return goapp.Button().Aria("label", "Stop workflow").Title("Stop workflow").
			Class(append(classes, "border-nord-11", "text-nord-11")...).OnClick(app.stop).Body(
			goapp.Span().Class("size-2.5", "rounded-xs", classBackgroundCurrent),
		)
	}

	return goapp.Button().Aria("label", "Compile and run workflow").Title("Compile and run workflow").
		Class(append(classes, "border-nord-10", "bg-nord-10", "text-white")...).
		Disabled(app.status != statusReady && app.status != statusFailed).OnClick(app.run).Body(
		goapp.Span().Class(
			"ml-0.5", "h-0", "w-0", "border-y-[6px]", "border-l-[10px]",
			"border-y-transparent", "border-l-current",
		),
	)
}

func (app *App) renderFileTree() goapp.UI {
	files := make([]goapp.UI, 0, len(exampleScripts))
	disabled := !app.mounted || app.status == statusCompiling || app.status == statusRunning

	for _, script := range exampleScripts {
		classes := []string{
			"flex", "w-full", classItemsCenter, "gap-2", "px-3", "py-1.5", "text-left", "font-mono",
			classTextExtraSmall, "text-nord-5", "hover:not-disabled:bg-nord-2",
			"disabled:cursor-not-allowed", "disabled:opacity-50",
		}

		selected := script.filename == app.selectedExample
		if selected {
			classes = append(classes, "bg-nord-2", "text-nord-8")
		}

		button := goapp.Button().Aria("label", "Open "+script.filename)
		if selected {
			button = button.Aria("current", "page")
		}

		files = append(files, goapp.Li().Body(
			button.DataSet("filename", script.filename).Class(classes...).Disabled(disabled).
				OnClick(app.selectExample(script.filename)).Body(
				goapp.Img().Src("web/images/go-logo.svg").Alt("").Aria("hidden", "true").
					Class("size-4", "shrink-0", "object-contain"),
				goapp.Span().Class("truncate").Text(script.filename),
			),
		))
	}

	return goapp.Nav().Aria("label", "Example workflows").DataSet(dataResizePanel, "files").
		Class(classMinWidth0, "overflow-y-auto", "bg-nord-0").Body(
		goapp.Ul().Class("py-2").Body(files...),
	)
}
