// Command compiler provides the Cord playground's Go-to-WebAssembly compilation service.
package main

import (
	"context"
	"errors"
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
	defaultMaxWASMBytes    = 32 << 20
	defaultMaxDiagnostics  = 64 << 10
	defaultTimeout         = 2 * time.Minute
	defaultWriteTimeout    = 30 * time.Second
	defaultConcurrency     = 2
	defaultCacheCapacity   = 2
	defaultCacheTTL        = 15 * time.Minute
	defaultAddress         = "127.0.0.1:8080"
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
	maxWASMBytes    int64
	maxDiagnostics  int
	compileTimeout  time.Duration
	writeTimeout    time.Duration
	maxConcurrency  int
	cacheCapacity   int
	cacheTTL        time.Duration
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, err); writeErr != nil {
			os.Exit(1)
		}

		os.Exit(1)
	}
}

func run(arguments []string) error {
	cfg, err := parseConfig(arguments)
	if err != nil {
		return err
	}

	compiler, err := newWASMCompiler(
		cfg.cordDirectory,
		cfg.maxWASMBytes,
		cfg.maxDiagnostics,
	)
	if err != nil {
		return err
	}

	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()

	server := newHTTPServer(&cfg, newHandlerWithContext(serviceContext, &cfg, compiler))

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
			cancelService()

			return fmt.Errorf("shut down compiler: %w", err)
		}

		cancelService()

		return nil
	}
}

func newHTTPServer(cfg *config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.address,
		Handler:           handler,
		ReadHeaderTimeout: serverHeaderTimeout,
		ReadTimeout:       serverHeaderTimeout,
		// The response deadline is set after compilation. An http.Server
		// WriteTimeout starts when the request headers are read, so a slow
		// compilation would consume the time reserved for sending the WASM.
		WriteTimeout: 0,
		IdleTimeout:  serverIdleTimeout,
	}
}
