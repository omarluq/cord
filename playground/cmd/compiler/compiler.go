package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	cordModule     = "github.com/omarluq/cord"
	moduleFileMode = 0o600
)

type compiler interface {
	// Compile builds Go source into a WebAssembly module.
	Compile(context.Context, string) ([]byte, error)
}

type wasmCompiler struct {
	cordDirectory string
	goDirectory   string
	goRoot        string
}

func newWASMCompiler(cordDirectory string) (*wasmCompiler, error) {
	cordPath, err := filepath.Abs(cordDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve Cord directory: %w", err)
	}
	goBinary, err := exec.LookPath("go")
	if err != nil {
		return nil, fmt.Errorf("find Go compiler: %w", err)
	}
	goBinary, err = filepath.Abs(goBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve Go compiler: %w", err)
	}
	goroot, err := goRoot(context.Background(), goBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve Go root: %w", err)
	}

	return &wasmCompiler{
		cordDirectory: cordPath,
		goDirectory:   filepath.Dir(goBinary),
		goRoot:        goroot,
	}, nil
}

// Compile builds source with the constrained playground toolchain.
func (compiler *wasmCompiler) Compile(ctx context.Context, source string) ([]byte, error) {
	directory, err := os.MkdirTemp("", "cord-playground-compile-")
	if err != nil {
		return nil, fmt.Errorf("create compilation directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(directory); err != nil {
			slog.Warn("remove compilation directory", "error", err)
		}
	}()

	module := fmt.Sprintf(`module playground.user

go 1.27rc2

require %s v0.0.0

replace %s => %s
`, cordModule, cordModule, filepath.ToSlash(compiler.cordDirectory))
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte(module), moduleFileMode); err != nil {
		return nil, fmt.Errorf("write module: %w", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "main.go"), []byte(source), moduleFileMode); err != nil {
		return nil, fmt.Errorf("write source: %w", err)
	}

	command := compiler.buildCommand(ctx)
	command.Dir = directory
	command.Env = append(
		environmentWithoutPath(),
		"GOOS=js",
		"GOARCH=wasm",
		"CGO_ENABLED=0",
		"GOROOT="+compiler.goRoot,
		"GOTOOLCHAIN=local",
		"GOCACHE="+filepath.Join(directory, "cache"),
		"GOPROXY=off",
		"GOSUMDB=off",
		"PATH="+compiler.goDirectory,
	)
	var diagnostics bytes.Buffer
	command.Stdout = &diagnostics
	command.Stderr = &diagnostics
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("compile context: %w", ctx.Err())
		}
		return nil, fmt.Errorf("compile source: %w: %s", err, diagnostics.String())
	}

	return readWASM(directory)
}

func readWASM(directory string) ([]byte, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, fmt.Errorf("open compilation directory: %w", err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			slog.Warn("close compilation root", "error", err)
		}
	}()
	outputFile, err := root.Open("app.wasm")
	if err != nil {
		return nil, fmt.Errorf("open WebAssembly: %w", err)
	}
	defer func() {
		if err := outputFile.Close(); err != nil {
			slog.Warn("close WebAssembly output", "error", err)
		}
	}()
	wasm, err := io.ReadAll(outputFile)
	if err != nil {
		return nil, fmt.Errorf("read WebAssembly: %w", err)
	}
	return wasm, nil
}

func (compiler *wasmCompiler) buildCommand(ctx context.Context) *exec.Cmd {
	return exec.CommandContext(
		ctx,
		"go",
		"build",
		"-mod=mod",
		"-trimpath",
		"-o",
		"app.wasm",
		".",
	)
}

func goRoot(ctx context.Context, goBinary string) (string, error) {
	output, err := exec.CommandContext(ctx, goBinary, "env", "GOROOT").Output()
	if err != nil {
		return "", fmt.Errorf("run go env GOROOT: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func environmentWithoutPath() []string {
	environment := os.Environ()
	filtered := make([]string, 0, len(environment))
	for _, variable := range environment {
		if !strings.HasPrefix(variable, "PATH=") {
			filtered = append(filtered, variable)
		}
	}
	return filtered
}
