// Command compiler provides the Cord playground's Go-to-WebAssembly compilation service.
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
	defaultMaxRequestBytes = 1 << 20
	defaultMaxSourceBytes  = 512 << 10
	defaultTimeout         = 2 * time.Minute
	defaultConcurrency     = 2
	serverHeaderTimeout    = 5 * time.Second
	serverIdleTimeout      = 30 * time.Second
	serverShutdownTimeout  = 5 * time.Second
)

type config struct {
	address         string
	allowedOrigin   string
	cordDirectory   string
	maxRequestBytes int64
	maxSourceBytes  int
	compileTimeout  time.Duration
	maxConcurrency  int
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	cfg, err := parseConfig(arguments)
	if err != nil {
		return err
	}

	compiler, err := newWASMCompiler(cfg.cordDirectory)
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:              cfg.address,
		Handler:           newHandler(cfg, compiler),
		ReadHeaderTimeout: serverHeaderTimeout,
		ReadTimeout:       serverHeaderTimeout,
		WriteTimeout:      cfg.compileTimeout + serverShutdownTimeout,
		IdleTimeout:       serverIdleTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	failures := make(chan error, 1)
	go func() {
		failures <- server.ListenAndServe()
	}()

	select {
	case err = <-failures:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve compiler: %w", err)
		}
		return nil
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return fmt.Errorf("shut down compiler: %w", err)
		}
		return nil
	}
}

func parseConfig(arguments []string) (config, error) {
	flags := flag.NewFlagSet("playground-compiler", flag.ContinueOnError)
	cfg := config{}
	flags.StringVar(&cfg.address, "addr", "127.0.0.1:8080", "listen address")
	flags.StringVar(&cfg.allowedOrigin, "allowed-origin", os.Getenv("CORD_COMPILER_ALLOWED_ORIGIN"), "allowed CORS origin")
	flags.StringVar(&cfg.cordDirectory, "cord-dir", ".", "Cord module directory")
	flags.Int64Var(&cfg.maxRequestBytes, "max-request-bytes", defaultMaxRequestBytes, "maximum JSON request size")
	flags.IntVar(&cfg.maxSourceBytes, "max-source-bytes", defaultMaxSourceBytes, "maximum source size")
	flags.DurationVar(&cfg.compileTimeout, "timeout", defaultTimeout, "compilation timeout")
	flags.IntVar(&cfg.maxConcurrency, "concurrency", defaultConcurrency, "maximum concurrent compilations")
	if err := flags.Parse(arguments); err != nil {
		return config{}, fmt.Errorf("parse flags: %w", err)
	}
	if cfg.maxRequestBytes < 1 || cfg.maxSourceBytes < 1 || cfg.compileTimeout <= 0 || cfg.maxConcurrency < 1 {
		return config{}, errors.New("request limits, timeout, and concurrency must be positive")
	}

	return cfg, nil
}
