//go:build !unix

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
)

func configureProcessGroup(_ *exec.Cmd) {}

func terminateProcessGroup(command *exec.Cmd) error {
	err := command.Process.Kill()
	if err == nil || errors.Is(err, os.ErrProcessDone) {
		return nil
	}

	return fmt.Errorf("kill compiler: %w", err)
}
