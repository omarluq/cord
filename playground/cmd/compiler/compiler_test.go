//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const helperModeEnv = "CORD_COMPILER_HELPER_MODE"

func TestLimitedBuffer(t *testing.T) {
	t.Parallel()

	buffer := newLimitedBuffer(4)
	written, err := buffer.Write([]byte("abcdef"))
	require.NoError(t, err)
	require.Equal(t, 6, written)
	require.Equal(t, "abcd\n[compiler diagnostics truncated]", buffer.String())
}

func TestReadWASMArtifactSizeBoundary(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	require.NoError(t, os.WriteFile(directory+"/app.wasm", []byte("wasm"), 0o600))

	tests := []struct {
		name      string
		wantError string
		limit     int64
	}{
		{name: "oversized", wantError: "WebAssembly exceeds 3-byte limit", limit: 3},
		{name: "exact limit", wantError: "", limit: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wasm, err := readWASM(directory, test.limit)
			if test.wantError != "" {
				require.EqualError(t, err, test.wantError)

				return
			}

			require.NoError(t, err)
			require.Equal(t, []byte("wasm"), wasm)
		})
	}
}

func TestRunCommandTerminatesProcessGroup(t *testing.T) {
	runCompilerHelper(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()

	command := exec.CommandContext(
		context.WithoutCancel(ctx),
		"/proc/self/exe",
		"-test.run=TestRunCommandTerminatesProcessGroup",
	)

	command.Env = append(os.Environ(), helperModeEnv+"=parent")

	var output bytes.Buffer

	command.Stdout = &output

	startedAt := time.Now()
	err := runCommand(ctx, command)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(startedAt), 2*time.Second)

	pid, conversionErr := strconv.Atoi(strings.TrimSpace(output.String()))
	require.NoError(t, conversionErr)
	require.Eventually(t, func() bool {
		processErr := syscall.Kill(pid, 0)

		return errors.Is(processErr, syscall.ESRCH)
	}, time.Second, 10*time.Millisecond)
}

func runCompilerHelper(t *testing.T) {
	t.Helper()

	switch os.Getenv(helperModeEnv) {
	case "":
		return
	case "parent":
		child := exec.CommandContext(
			t.Context(),
			"/proc/self/exe",
			"-test.run=TestRunCommandTerminatesProcessGroup",
		)

		child.Env = append(os.Environ(), helperModeEnv+"=child")
		child.Stdout = os.Stdout
		require.NoError(t, child.Start())

		_, err := fmt.Fprintln(os.Stdout, child.Process.Pid)
		require.NoError(t, err)
		require.NoError(t, child.Wait())
		os.Exit(0)
	case "child":
		select {}
	default:
		t.Fatalf("unknown helper mode %q", os.Getenv(helperModeEnv))
	}
}
