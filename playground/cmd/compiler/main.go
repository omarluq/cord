// Command compiler provides the Cord playground's Go-to-WebAssembly compilation service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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

func parseConfig(arguments []string) (config, error) {
	address, err := addressFromEnvironment()
	if err != nil {
		return config{}, err
	}

	flags := flag.NewFlagSet("playground-compiler", flag.ContinueOnError)
	cfg := config{}
	flags.StringVar(&cfg.address, "addr", address, "listen address")
	flags.StringVar(
		&cfg.allowedOrigin,
		"allowed-origin",
		os.Getenv("CORD_COMPILER_ALLOWED_ORIGIN"),
		"allowed CORS origin",
	)
	flags.StringVar(&cfg.cordDirectory, "cord-dir", ".", "Cord module directory")
	flags.Int64Var(&cfg.maxRequestBytes, "max-request-bytes", defaultMaxRequestBytes, "maximum JSON request size")
	flags.IntVar(&cfg.maxSourceBytes, "max-source-bytes", defaultMaxSourceBytes, "maximum source size")
	flags.Int64Var(&cfg.maxWASMBytes, "max-wasm-bytes", defaultMaxWASMBytes, "maximum compiled WebAssembly size")
	flags.IntVar(
		&cfg.maxDiagnostics,
		"max-diagnostics-bytes",
		defaultMaxDiagnostics,
		"maximum compiler diagnostics size",
	)
	flags.DurationVar(&cfg.compileTimeout, "timeout", defaultTimeout, "compilation timeout")
	flags.DurationVar(
		&cfg.writeTimeout,
		"write-timeout",
		defaultWriteTimeout,
		"compiled artifact response write timeout",
	)
	flags.IntVar(
		&cfg.maxConcurrency,
		"concurrency",
		defaultConcurrency,
		"maximum concurrent compilations",
	)
	flags.IntVar(
		&cfg.cacheCapacity,
		"cache-capacity",
		defaultCacheCapacity,
		"maximum cached compilation artifacts",
	)
	flags.DurationVar(
		&cfg.cacheTTL,
		"cache-ttl",
		defaultCacheTTL,
		"compiled artifact cache lifetime",
	)

	if err := flags.Parse(arguments); err != nil {
		return config{}, fmt.Errorf("parse flags: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func validateConfig(cfg *config) error {
	if err := validateSizeLimits(cfg); err != nil {
		return err
	}

	if cfg.compileTimeout <= 0 || cfg.writeTimeout <= 0 || cfg.maxConcurrency < 1 {
		return errors.New("timeouts and concurrency must be positive")
	}

	if cfg.cacheCapacity < 1 || cfg.cacheTTL <= 0 {
		return errors.New("cache settings must be positive")
	}

	return nil
}

func validateSizeLimits(cfg *config) error {
	if cfg.maxRequestBytes < 1 || cfg.maxSourceBytes < 1 ||
		cfg.maxWASMBytes < 1 || cfg.maxDiagnostics < 1 {
		return errors.New("size limits must be positive")
	}

	if cfg.maxWASMBytes == math.MaxInt64 {
		return errors.New("maximum WebAssembly size is too large")
	}

	return nil
}

func addressFromEnvironment() (string, error) {
	port := os.Getenv("PORT")
	if port == "" {
		return defaultAddress, nil
	}

	value, err := strconv.ParseUint(port, 10, 16)
	if err != nil || value == 0 {
		return "", fmt.Errorf("invalid PORT %q", port)
	}

	return fmt.Sprintf("0.0.0.0:%d", value), nil
}
