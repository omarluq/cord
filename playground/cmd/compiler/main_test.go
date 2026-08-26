package main

import (
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
