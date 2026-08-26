package main

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseConfigSetsWriteTimeout(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{"-write-timeout", "7s"})
	require.NoError(t, err)
	require.Equal(t, 7*time.Second, cfg.writeTimeout)
}

func TestValidateConfigRejectsNonpositiveWriteTimeout(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.writeTimeout = 0

	require.EqualError(t, validateConfig(&cfg), "timeouts and concurrency must be positive")
}

func TestValidateConfigRejectsMaximumWASMSizeOverflow(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.maxWASMBytes = math.MaxInt64

	require.EqualError(t, validateConfig(&cfg), "maximum WebAssembly size is too large")
}

func TestAddressFromEnvironment(t *testing.T) {
	t.Setenv("PORT", "8080")

	address, err := addressFromEnvironment()
	require.NoError(t, err)
	require.Equal(t, "0.0.0.0:8080", address)
}

func TestAddressFromEnvironmentRejectsInvalidPort(t *testing.T) {
	for _, port := range []string{"invalid", "0", "65536"} {
		t.Run(port, func(t *testing.T) {
			t.Setenv("PORT", port)

			_, err := addressFromEnvironment()
			require.Error(t, err)
		})
	}
}
