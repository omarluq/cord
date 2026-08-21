package playground

import (
	goapp "github.com/maxence-charriere/go-app/v11/pkg/app"
)

const (
	classBackgroundCurrent = "bg-current"
	classBorderNord3       = "border-nord-3"
	classCursorPointer     = "cursor-pointer"
	classFlex1             = "flex-1"
	classFlexColumn        = "flex-col"
	classItemsCenter       = "items-center"
	classMinHeight0        = "min-h-0"
	classOverflowHidden    = "overflow-hidden"
	classTextExtraSmall    = "text-xs"
)

func panelClasses() []string {
	return []string{
		classOverflowHidden,
		"border",
		classBorderNord3,
		"bg-nord-1",
	}
}

func buttonClasses() []string {
	return []string{
		classCursorPointer,
		"rounded-md",
		"border",
		classBorderNord3,
		"bg-nord-2",
		"px-3",
		"py-2",
		classTextExtraSmall,
		"font-semibold",
		"transition-colors",
		"hover:not-disabled:border-nord-8",
		"disabled:cursor-not-allowed",
		"disabled:opacity-40",
	}
}

// Render builds the editor, graph, and output interface.
func (app *App) Render() goapp.UI {
	return goapp.Main().Class(
		"flex",
		"h-dvh",
		classMinHeight0,
		classFlexColumn,
		classOverflowHidden,
	).Body(
		goapp.Div().Class(
			"grid",
			classMinHeight0,
			"w-full",
			classFlex1,
			"grid-cols-2",
			classOverflowHidden,
		).Body(
			app.renderEditorPanel(),
			app.renderResultsPanel(),
		),
	)
}

func (app *App) renderEditorPanel() goapp.UI {
	return goapp.Section().Class(append(
		panelClasses(),
		"flex",
		classMinHeight0,
	)...).Body(
		app.renderFileTree(),
		goapp.Div().Class(
			"flex",
			classMinHeight0,
			"min-w-0",
			classFlex1,
			classFlexColumn,
		).Body(
			goapp.Div().Class(
				"flex",
				"shrink-0",
				classItemsCenter,
				"justify-end",
				"border-b",
				classBorderNord3,
				"px-4",
				"py-3",
			).Body(
				goapp.Span().
					DataSet("testid", "status").
					Class("sr-only").
					Aria("live", "polite").
					Text(string(app.status)),
				app.renderExecutionButton(),
			),
			goapp.Div().ID(editorID).Class(classMinHeight0, classFlex1),
		),
	)
}

func (app *App) renderResultsPanel() goapp.UI {
	return goapp.Div().Class(
		"grid",
		classMinHeight0,
		"min-w-0",
		"grid-rows-[minmax(0,2fr)_minmax(0,1fr)]",
		classOverflowHidden,
	).Body(
		app.renderGraphPanel(),
		goapp.Section().Class(append(
			panelClasses(),
			"flex",
			classMinHeight0,
			classFlexColumn,
		)...).Body(
			goapp.Div().Class(
				"shrink-0",
				"border-b",
				classBorderNord3,
				"px-4",
				"py-3",
			).Body(
				goapp.H2().Class(
					classTextExtraSmall,
					"font-bold",
					"uppercase",
					"tracking-wider",
				).Text("Output"),
			),
			goapp.Pre().
				DataSet("testid", "run-result").
				Class(
					classMinHeight0,
					classFlex1,
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
		panelClasses(),
		"relative",
		"flex",
		classMinHeight0,
		classFlexColumn,
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
			classMinHeight0,
			classFlex1,
			"bg-nord-0",
		),
	)
}

func (app *App) renderZoomButton(direction string, amount int) goapp.UI {
	label := "Zoom " + direction + " workflow graph"

	icon := []goapp.UI{
		goapp.Span().Class(
			"absolute",
			"h-0.5",
			"w-3",
			"rounded-full",
			classBackgroundCurrent,
		),
	}
	if direction == "in" {
		icon = append(
			icon,
			goapp.Span().Class(
				"absolute",
				"h-3",
				"w-0.5",
				"rounded-full",
				classBackgroundCurrent,
			),
		)
	}

	return goapp.Button().
		Aria("label", label).
		Title("Zoom "+direction).
		Disabled(!app.mounted).
		Class(
			"relative",
			"flex",
			"size-8",
			classCursorPointer,
			classItemsCenter,
			"justify-center",
			"rounded",
			"border",
			classBorderNord3,
			"bg-nord-1/90",
			"text-nord-5",
			"hover:border-nord-8",
			"hover:text-nord-8",
		).
		OnClick(func(goapp.Context, goapp.Event) {
			app.bridge.zoomGraph(amount)
		}).
		Body(icon...)
}

func (app *App) renderExecutionButton() goapp.UI {
	classes := append(
		buttonClasses(),
		"flex",
		"size-9",
		classItemsCenter,
		"justify-center",
		"p-0",
	)
	if app.status == statusRunning {
		return goapp.Button().
			Aria("label", "Stop workflow").
			Title("Stop workflow").
			Class(append(
				classes,
				"border-nord-11",
				"text-nord-11",
			)...).
			OnClick(app.stop).
			Body(
				goapp.Span().Class(
					"size-2.5",
					"rounded-xs",
					classBackgroundCurrent,
				),
			)
	}

	return goapp.Button().
		Aria("label", "Compile and run workflow").
		Title("Compile and run workflow").
		Class(append(
			classes,
			"border-nord-10",
			"bg-nord-10",
			"text-white",
		)...).
		Disabled(
			app.status != statusReady &&
				app.status != statusFailed,
		).
		OnClick(app.run).
		Body(
			goapp.Span().Class(
				"ml-0.5",
				"h-0",
				"w-0",
				"border-y-[6px]",
				"border-l-[10px]",
				"border-y-transparent",
				"border-l-current",
			),
		)
}

func (app *App) renderFileTree() goapp.UI {
	files := make([]goapp.UI, 0, len(exampleScripts))
	disabled := !app.mounted || app.status == statusCompiling || app.status == statusRunning

	for _, script := range exampleScripts {
		classes := []string{
			"flex",
			"w-full",
			classItemsCenter,
			"gap-2",
			"px-3",
			"py-1.5",
			"text-left",
			"font-mono",
			classTextExtraSmall,
			"text-nord-5",
			"hover:not-disabled:bg-nord-2",
			"disabled:cursor-not-allowed",
			"disabled:opacity-50",
		}
		if script.filename == app.selectedExample {
			classes = append(classes, "bg-nord-2", "text-nord-8")
		}

		files = append(files, goapp.Li().Body(
			goapp.Button().
				Aria("label", "Open "+script.filename).
				DataSet("filename", script.filename).
				Class(classes...).
				Disabled(disabled).
				OnClick(app.selectExample(script.filename)).
				Body(
					goapp.Img().
						Src("web/images/go-logo.svg").
						Alt("").
						Aria("hidden", "true").
						Class("size-4", "shrink-0", "object-contain"),
					goapp.Span().Class("truncate").Text(script.filename),
				),
		))
	}

	return goapp.Nav().
		Aria("label", "Example workflows").
		Class(
			"w-48",
			"shrink-0",
			"overflow-y-auto",
			"border-r",
			classBorderNord3,
			"bg-nord-0",
		).Body(
		goapp.Div().Class(
			"border-b",
			classBorderNord3,
			"px-3",
			"py-3",
			classTextExtraSmall,
			"font-bold",
			"uppercase",
			"tracking-wider",
			"text-nord-4",
		).Text("Examples"),
		goapp.Ul().Class("py-2").Body(files...),
	)
}
