//go:build !unix

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
)

const processGroupsSupported = false

func configureProcessGroup(_ *exec.Cmd) error {
	return errors.New("compiler process groups are unsupported")
}

func terminateProcessGroup(command *exec.Cmd) error {
	err := command.Process.Kill()
	if err == nil || errors.Is(err, os.ErrProcessDone) {
		return nil
	}

	return fmt.Errorf("kill compiler: %w", err)
}
