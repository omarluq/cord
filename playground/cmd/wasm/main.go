//go:build js && wasm

// Command wasm starts the Cord playground in a browser.
package main

import (
	goapp "github.com/maxence-charriere/go-app/v10/pkg/app"
	"github.com/omarluq/cord/playground"
)

func main() {
	playground.RegisterRoute()
	goapp.RunWhenOnBrowser()
}
