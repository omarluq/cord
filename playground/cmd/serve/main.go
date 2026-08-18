// Command serve serves a static playground build for local development and tests.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	shutdownTimeout  = 5 * time.Second
	readTimeout      = 5 * time.Second
	startupPollDelay = 10 * time.Millisecond
	directoryMode    = 0o750
	fileMode         = 0o600
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, err); writeErr != nil {
			os.Exit(1)
		}

		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("playground-serve", flag.ContinueOnError)
	directory := flags.String("dir", "dist/playground", "static site directory")
	address := flags.String("addr", "127.0.0.1:4173", "listen address")
	readyFile := flags.String("ready-file", "", "file written after the server starts")

	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	server := &http.Server{
		Addr: *address, Handler: contentTypes(http.FileServer(http.Dir(*directory))), ReadHeaderTimeout: readTimeout,
	}

	failures := serve(server)
	if err := waitUntilServing(*address, failures); err != nil {
		return err
	}

	cleanup, err := signalReady(*readyFile, *address)
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err = fmt.Fprintf(os.Stdout, "Cord playground: http://%s\n", *address); err != nil {
		return fmt.Errorf("write server address: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err = <-failures:
		if err != nil {
			return fmt.Errorf("serve playground: %w", err)
		}

		return nil
	case <-ctx.Done():
		return shutdown(server)
	}
}

func serve(server *http.Server) <-chan error {
	failures := make(chan error, 1)

	go func() {
		defer close(failures)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failures <- err
		}
	}()

	return failures
}

func signalReady(filename, address string) (func(), error) {
	const (
		readyDirectoryMode = 0o750
		readyFileMode      = 0o600
	)

	if filename != "" {
		if err := os.MkdirAll(".tmp", readyDirectoryMode); err != nil {
			return nil, fmt.Errorf("create ready directory: %w", err)
		}

		if err := os.WriteFile(filename, []byte(address), readyFileMode); err != nil {
			return nil, fmt.Errorf("write ready file: %w", err)
		}
	}

	return func() {
		if filename == "" {
			return
		}

		if err := os.Remove(filename); err != nil && !errors.Is(err, os.ErrNotExist) {
			if _, writeErr := fmt.Fprintf(
				os.Stderr,
				"remove ready file: %v\n",
				err,
			); writeErr != nil {
				return
			}
		}
	}, nil
}

func shutdown(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shut down playground server: %w", err)
	}

	return nil
}

func contentTypes(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/web/app.wasm":
			response.Header().Set("Content-Type", "application/wasm")
		case "/web/playground.css":
			response.Header().Set("Content-Type", "text/css; charset=utf-8")
		}

		next.ServeHTTP(response, request)
	})
}

func waitUntilServing(address string, failures <-chan error) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ticker := time.NewTicker(startupPollDelay)
	defer ticker.Stop()

	for {
		select {
		case err := <-failures:
			return fmt.Errorf("serve playground: %w", err)
		case <-ctx.Done():
			return errors.New("playground server did not start")
		case <-ticker.C:
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address, http.NoBody)
			if err != nil {
				return fmt.Errorf("create startup request: %w", err)
			}

			response, err := http.DefaultClient.Do(request)
			if err != nil {
				continue
			}

			if err := response.Body.Close(); err != nil {
				return fmt.Errorf("close startup response: %w", err)
			}

			return nil
		}
	}
}
