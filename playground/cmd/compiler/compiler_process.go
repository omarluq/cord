package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

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

func goEnvironment(goBinary string) (string, error) {
	command := &exec.Cmd{
		Path: goBinary,
		Args: []string{goBinary, "env", "GOROOT"},
	}

	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("run go env: %w", err)
	}

	goRoot := strings.TrimSpace(string(output))
	if goRoot == "" {
		return "", errors.New("go env returned an invalid GOROOT")
	}

	return goRoot, nil
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
