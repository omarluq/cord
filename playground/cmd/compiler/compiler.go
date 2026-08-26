package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

const moduleFileMode = 0o600

type compiler interface {
	// Compile builds Go source into a WebAssembly module.
	Compile(context.Context, string) ([]byte, error)
}

type wasmCompiler struct {
	cordDirectory  string
	goBinary       string
	goRoot         string
	maxWASMBytes   int64
	maxDiagnostics int
}

func newWASMCompiler(
	cordDirectory string,
	maxWASMBytes int64,
	maxDiagnostics int,
) (*wasmCompiler, error) {
	if !processGroupsSupported {
		return nil, fmt.Errorf("compiler service does not support %s", runtime.GOOS)
	}

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

	goRoot, err := goEnvironment(goBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve Go environment: %w", err)
	}

	return &wasmCompiler{
		cordDirectory:  cordPath,
		goBinary:       goBinary,
		goRoot:         goRoot,
		maxWASMBytes:   maxWASMBytes,
		maxDiagnostics: maxDiagnostics,
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

	module := moduleSource(compiler.cordDirectory)
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
		"GOCACHE="+filepath.Join(directory, "go-build-cache"),
		"GOPROXY=off",
		"GOSUMDB=off",
	)

	diagnostics := newLimitedBuffer(compiler.maxDiagnostics)

	command.Stdout = diagnostics
	command.Stderr = diagnostics

	if err := runCommand(ctx, command); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("compile context: %w", ctx.Err())
		}

		return nil, fmt.Errorf("compile source: %w: %s", err, diagnostics.String())
	}

	return readWASM(directory, compiler.maxWASMBytes)
}

func (compiler *wasmCompiler) buildCommand(_ context.Context) *exec.Cmd {
	return &exec.Cmd{
		Path: compiler.goBinary,
		Args: []string{
			compiler.goBinary,
			"build",
			"-mod=mod",
			"-trimpath",
			"-o",
			"app.wasm",
			".",
		},
	}
}
