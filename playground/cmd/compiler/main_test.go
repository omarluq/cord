package main

import (
	"math"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPServerSetsResponseDeadlineAfterCompilation(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	server := newHTTPServer(&cfg, http.NotFoundHandler())

	require.Zero(t, server.WriteTimeout)
	require.Equal(t, serverHeaderTimeout, server.ReadHeaderTimeout)
	require.Equal(t, serverHeaderTimeout, server.ReadTimeout)
	require.Equal(t, serverIdleTimeout, server.IdleTimeout)
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
