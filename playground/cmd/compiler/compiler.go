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
)

const (
	cordModule       = "github.com/omarluq/cord"
	playgroundModule = "github.com/omarluq/cord/playground"
	moduleFileMode   = 0o600
)

type compiler interface {
	Compile(context.Context, string) ([]byte, error)
}

type wasmCompiler struct {
	cordDirectory string
	playDirectory string
}

func newWASMCompiler(cordDirectory, playDirectory string) (*wasmCompiler, error) {
	cordPath, err := filepath.Abs(cordDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve Cord directory: %w", err)
	}
	playPath, err := filepath.Abs(playDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve playground directory: %w", err)
	}
	if _, err := exec.LookPath("go"); err != nil {
		return nil, fmt.Errorf("find Go compiler: %w", err)
	}

	return &wasmCompiler{cordDirectory: cordPath, playDirectory: playPath}, nil
}

func (compiler *wasmCompiler) Compile(ctx context.Context, source string) (wasm []byte, resultErr error) {
	directory, err := os.MkdirTemp("", "cord-playground-compile-")
	if err != nil {
		return nil, fmt.Errorf("create compilation directory: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(directory)) }()

	module := fmt.Sprintf(`module playground.user

go 1.27rc2

require (
	%s v0.0.0
	%s v0.0.0
)

replace %s => %s
replace %s => %s
`, cordModule, playgroundModule, cordModule, filepath.ToSlash(compiler.cordDirectory),
		playgroundModule, filepath.ToSlash(compiler.playDirectory))
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte(module), moduleFileMode); err != nil {
		return nil, fmt.Errorf("write module: %w", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "main.go"), []byte(source), moduleFileMode); err != nil {
		return nil, fmt.Errorf("write source: %w", err)
	}

	command := exec.CommandContext(ctx, "go", "build", "-mod=mod", "-trimpath", "-o", "app.wasm", ".")
	command.Dir = directory
	command.Env = append(
		os.Environ(),
		"GOOS=js",
		"GOARCH=wasm",
		"CGO_ENABLED=0",
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
