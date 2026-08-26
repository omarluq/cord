package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
)

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
