package playground

import (
	goapp "github.com/maxence-charriere/go-app/v10/pkg/app"
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
		classFlexColumn,
	)...).Body(
		goapp.Div().Class(
			"flex",
			"shrink-0",
			classItemsCenter,
			"justify-between",
			"gap-4",
			"border-b",
			classBorderNord3,
			"px-4",
			"py-3",
		).Body(
			app.renderExampleSelect(),
			goapp.Div().Class("flex", classItemsCenter, "gap-3").Body(
				goapp.Span().
					DataSet("testid", "status").
					Class("sr-only").
					Aria("live", "polite").
					Text(string(app.status)),
				app.renderExecutionButton(),
			),
		),
		goapp.Div().ID(editorID).Class(classMinHeight0, classFlex1),
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
				classCursorPointer,
				"appearance-none",
				"rounded",
				"border",
				classBorderNord3,
				"bg-nord-0",
				"py-1",
				"pr-2",
				"pl-8",
				"font-mono",
				classTextExtraSmall,
				"text-nord-5",
			).
			Disabled(
				!app.mounted ||
					app.status == statusCompiling ||
					app.status == statusRunning,
			).
			OnChange(app.selectExample).
			Body(options...),
		goapp.Img().
			Src("web/images/go-logo.svg").
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
