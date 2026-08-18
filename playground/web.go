// Package playground implements Cord's static browser playground.
package playground

import (
	_ "embed" // Embed the static browser assets into the site generator.
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	goapp "github.com/maxence-charriere/go-app/v10/pkg/app"
)

//go:embed web/icon.svg
var icon []byte

//go:embed web/go-logo.svg
var goLogo []byte

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
	output, err := safeOutputPath(output)
	if err != nil {
		return err
	}

	RegisterRoute()
	if err := os.RemoveAll(output); err != nil {
		return fmt.Errorf("clear static output: %w", err)
	}
	if err := goapp.GenerateStaticWebsite(output, Handler(prefix)); err != nil {
		return fmt.Errorf("generate static website: %w", err)
	}

	assets := map[string][]byte{
		"icon.svg":    icon,
		"go-logo.svg": goLogo,
	}
	for name, content := range assets {
		asset := filepath.Join(output, "web", name)
		if err := os.WriteFile(asset, content, assetMode); err != nil {
			return fmt.Errorf("write playground asset %q: %w", name, err)
		}
	}

	return nil
}

func safeOutputPath(output string) (string, error) {
	if output == "" {
		return "", errors.New("static output path is empty")
	}
	absolute, err := filepath.Abs(output)
	if err != nil {
		return "", fmt.Errorf("resolve static output: %w", err)
	}
	absolute = filepath.Clean(absolute)
	if filepath.Dir(absolute) == absolute {
		return "", fmt.Errorf("static output path %q is a filesystem root", output)
	}

	workingDirectory, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}
	relative, err := filepath.Rel(absolute, workingDirectory)
	if err != nil {
		return "", fmt.Errorf("compare static output with working directory: %w", err)
	}
	if relative == "." ||
		(relative != ".." &&
			!strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return "", fmt.Errorf("static output path %q contains the working directory", output)
	}
	return absolute, nil
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
