package playground

import goapp "github.com/maxence-charriere/go-app/v11/pkg/app"

func (app *App) mount(ctx *goapp.Context) {
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
					app.mountBridge(&ctx, generation)
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

func (app *App) nextGeneration() uint64 {
	app.generation++

	return app.generation
}

func (app *App) callbackIsCurrent(generation uint64, requireMounted bool) bool {
	return app.active && app.generation == generation && (!requireMounted || app.mounted)
}

func (app *App) mountBridge(ctx *goapp.Context, generation uint64) {
	if !app.callbackIsCurrent(generation, false) {
		return
	}

	source, ok := exampleSource(app.selectedExample)
	if !ok {
		source = linearSource
	}

	app.bridge.mountEditor(editorID, source)
	app.bridge.mountGraph(graphID)
	app.mounted = true

	ctx.Update()
}

func (app *App) cancelCompilation() {
	if app.compilationCancel == nil {
		return
	}

	app.compilationCancel()
	app.compilationCancel = nil
}
