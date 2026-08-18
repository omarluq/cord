package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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
	goBinary      string
	goRoot        string
	goCache       string
}

func newWASMCompiler(cordDirectory string) (*wasmCompiler, error) {
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

	goRoot, goCache, err := goEnvironment(goBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve Go environment: %w", err)
	}

	return &wasmCompiler{
		cordDirectory: cordPath,
		goBinary:      goBinary,
		goRoot:        goRoot,
		goCache:       goCache,
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
		"GOCACHE="+compiler.goCache,
		"GOPROXY=off",
		"GOSUMDB=off",
	)

	var diagnostics bytes.Buffer

	command.Stdout = &diagnostics

	command.Stderr = &diagnostics
	if err := runCommand(ctx, command); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("compile context: %w", ctx.Err())
		}

		return nil, fmt.Errorf("compile source: %w: %s", err, diagnostics.String())
	}

	return readWASM(directory)
}

func moduleSource(cordDirectory string) string {
	return fmt.Sprintf(`module playground.user

go 1.27rc2

require %s v0.0.0

replace %s => %s
`, cordModule, cordModule, strconv.Quote(filepath.ToSlash(cordDirectory)))
}

func readWASM(directory string) ([]byte, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, fmt.Errorf("open compilation directory: %w", err)
	}
	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			slog.Warn("close compilation root", "error", closeErr)
		}
	}()

	outputFile, err := root.Open("app.wasm")
	if err != nil {
		return nil, fmt.Errorf("open WebAssembly: %w", err)
	}

	defer func() {
		if closeErr := outputFile.Close(); closeErr != nil {
			slog.Warn("close WebAssembly output", "error", closeErr)
		}
	}()

	wasm, err := io.ReadAll(outputFile)
	if err != nil {
		return nil, fmt.Errorf("read WebAssembly: %w", err)
	}

	return wasm, nil
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

func runCommand(ctx context.Context, command *exec.Cmd) error {
	if err := configureProcessGroup(command); err != nil {
		return fmt.Errorf("configure compiler process group: %w", err)
	}

	if err := command.Start(); err != nil {
		return fmt.Errorf("start compiler: %w", err)
	}

	completed := make(chan error, 1)
	go func() {
		completed <- command.Wait()
	}()

	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
		terminationErr := terminateProcessGroup(command)
		waitErr := <-completed

		return errors.Join(ctx.Err(), terminationErr, waitErr)
	}
}

func goEnvironment(goBinary string) (
	goRoot string,
	goCache string,
	err error,
) {
	command := &exec.Cmd{
		Path: goBinary,
		Args: []string{goBinary, "env", "GOROOT", "GOCACHE"},
	}

	output, err := command.Output()
	if err != nil {
		return "", "", fmt.Errorf("run go env: %w", err)
	}

	values := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(values) != 2 || values[0] == "" || values[1] == "" {
		return "", "", errors.New("go env returned invalid GOROOT or GOCACHE")
	}

	return values[0], values[1], nil
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
