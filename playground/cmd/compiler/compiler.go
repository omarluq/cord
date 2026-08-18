package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
	Compile(context.Context, string) ([]byte, error)
}

type wasmCompiler struct {
	cordDirectory string
	goRoot        string
}

func newWASMCompiler(cordDirectory string) (*wasmCompiler, error) {
	cordPath, err := filepath.Abs(cordDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve Cord directory: %w", err)
	}
	if _, err := exec.LookPath("go"); err != nil {
		return nil, fmt.Errorf("find Go compiler: %w", err)
	}
	goroot, err := goRoot(context.Background())
	if err != nil {
		return nil, fmt.Errorf("resolve Go root: %w", err)
	}

	return &wasmCompiler{cordDirectory: cordPath, goRoot: goroot}, nil
}

func (compiler *wasmCompiler) Compile(ctx context.Context, source string) (wasm []byte, resultErr error) {
	directory, err := os.MkdirTemp("", "cord-playground-compile-")
	if err != nil {
		return nil, fmt.Errorf("create compilation directory: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(directory)) }()

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
		os.Environ(),
		"GOOS=js",
		"GOARCH=wasm",
		"CGO_ENABLED=0",
		"GOROOT="+compiler.goRoot,
		"GOTOOLCHAIN=local",
		"GOPROXY=off",
		"GOSUMDB=off",
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

	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, fmt.Errorf("open compilation directory: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	outputFile, err := root.Open("app.wasm")
	if err != nil {
		return nil, fmt.Errorf("open WebAssembly: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, outputFile.Close()) }()
	wasm, err = io.ReadAll(outputFile)
	if err != nil {
		return nil, fmt.Errorf("read WebAssembly: %w", err)
	}
	return wasm, nil
}

func (compiler *wasmCompiler) buildCommand(ctx context.Context) *exec.Cmd {
	return exec.CommandContext(ctx, "go", "build", "-mod=mod", "-trimpath", "-o", "app.wasm", ".")
}

func goRoot(ctx context.Context) (string, error) {
	output, err := exec.CommandContext(ctx, "go", "env", "GOROOT").Output()
	if err != nil {
		return "", fmt.Errorf("run go env GOROOT: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}
