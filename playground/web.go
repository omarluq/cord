// Package playground implements Cord's static browser playground.
package playground

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	goapp "github.com/maxence-charriere/go-app/v10/pkg/app"
)

//go:embed web/playground.css
var stylesheet []byte

//go:embed web/icon.svg
var icon []byte

//go:embed web/go-logo.svg
var goLogo []byte

//go:embed web/playground.js
var playgroundScript []byte

//go:embed web/worker.js
var workerScript []byte

const (
	route     = "/"
	iconPath  = "/web/icon.svg"
	assetMode = 0o600
)

// RegisterRoute registers the playground root component with go-app.
func RegisterRoute() {
	goapp.Route(route, func() goapp.Composer { return NewApp() })
}

// GenerateStatic assembles the playground website in output.
func GenerateStatic(output, prefix string) error {
	RegisterRoute()
	if err := os.RemoveAll(output); err != nil {
		return fmt.Errorf("clear static output: %w", err)
	}
	if err := goapp.GenerateStaticWebsite(output, Handler(prefix)); err != nil {
		return fmt.Errorf("generate static website: %w", err)
	}

	assets := map[string][]byte{
		"playground.css": stylesheet,
		"icon.svg":       icon,
		"go-logo.svg":    goLogo,
		"playground.js":  playgroundScript,
		"worker.js":      workerScript,
	}
	for name, content := range assets {
		asset := filepath.Join(output, "web", name)
		if err := os.WriteFile(asset, content, assetMode); err != nil {
			return fmt.Errorf("write playground asset %q: %w", name, err)
		}
	}

	return nil
}

// Handler returns the go-app static-site configuration for the given asset prefix.
func Handler(prefix string) *goapp.Handler {
	resources := goapp.PrefixedLocation(prefix)

	return &goapp.Handler{
		Name:            "Cord Playground",
		ShortName:       "Cord",
		Title:           "Cord Playground · Durable Go Workflows",
		Description:     "Explore durable typed Go workflows directly in your browser.",
		Author:          "Cord contributors",
		Icon:            goapp.Icon{Default: iconPath, SVG: iconPath},
		BackgroundColor: "#2e3440",
		ThemeColor:      "#2e3440",
		LoadingLabel:    "Loading Cord {progress}%",
		Lang:            "en",
		Styles:          []string{"/web/playground.css"},
		Scripts:         []string{"/web/playground.js defer"},
		CacheableResources: []string{
			"/web/playground.css", iconPath, "/web/go-logo.svg",
			"/web/playground.js", "/web/worker.js",
		},
		Resources: resources,
		StartURL:  route,
	}
}
