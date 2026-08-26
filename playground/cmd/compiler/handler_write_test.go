package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandlerReturnsErrorWhenResponseDeadlinesAreUnsupported(t *testing.T) {
	t.Parallel()

	handler := newHandler(
		testConfigPointer(),
		compilerFunc(func(context.Context, string) ([]byte, error) {
			return []byte("wasm"), nil
		}),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, compileRequestForTest(t.Context()))

	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.JSONEq(t, `{"error":"compiler response unavailable"}`, response.Body.String())
	require.NotContains(t, response.Body.String(), "wasm")
}

func TestHandlerBoundsStalledAndSlowReaders(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		readBytes int
	}{
		{name: "stalled", readBytes: 0},
		{name: "slow", readBytes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testHandlerBoundedReader(t, test.readBytes)
		})
	}
}

func testHandlerBoundedReader(t *testing.T, readBytes int) {
	t.Helper()

	cfg := testConfig()
	cfg.writeTimeout = 25 * time.Millisecond
	cfg.maxSourceBytes = 64
	artifact := bytes.Repeat([]byte("wasm"), 2<<20)
	handler := newHandler(&cfg, compilerFunc(func(context.Context, string) ([]byte, error) {
		return artifact, nil
	}))

	serverConnection, clientConnection := net.Pipe()
	handlerDone := make(chan struct{})
	server := &http.Server{
		Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			handler.ServeHTTP(response, request)
			close(handlerDone)
		}),
		ReadHeaderTimeout: time.Second,
	}
	listener := &singleConnectionListener{
		connection: serverConnection,
		closed:     make(chan struct{}),
		mu:         sync.Mutex{},
		accepted:   false,
		close:      sync.Once{},
	}
	serveDone := make(chan error, 1)

	go func() { serveDone <- server.Serve(listener) }()

	t.Cleanup(func() {
		require.NoError(t, clientConnection.Close())
		require.NoError(t, server.Close())
		require.ErrorIs(t, <-serveDone, http.ErrServerClosed)
	})

	const requestHeaders = "POST /compile HTTP/1.1\r\n" +
		"Host: compiler\r\nContent-Type: application/json\r\n" +
		"Content-Length: %d\r\nConnection: close\r\n\r\n%s"

	_, err := fmt.Fprintf(clientConnection, requestHeaders, len(compileRequestBody), compileRequestBody)
	require.NoError(t, err)

	if readBytes > 0 {
		go slowlyReadConnection(clientConnection, readBytes)
	}

	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("compiler server retained a response past its write deadline")
	}
}

func slowlyReadConnection(connection net.Conn, readBytes int) {
	buffer := make([]byte, readBytes)

	for {
		if _, err := io.ReadFull(connection, buffer); err != nil {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}
}

type singleConnectionListener struct {
	connection net.Conn
	closed     chan struct{}

	mu       sync.Mutex
	accepted bool
	close    sync.Once
}

func (listener *singleConnectionListener) Accept() (net.Conn, error) {
	listener.mu.Lock()
	if !listener.accepted {
		listener.accepted = true
		listener.mu.Unlock()

		return listener.connection, nil
	}
	listener.mu.Unlock()

	<-listener.closed

	return nil, net.ErrClosed
}

func (listener *singleConnectionListener) Close() error {
	listener.close.Do(func() { close(listener.closed) })

	return nil
}

func (*singleConnectionListener) Addr() net.Addr { return pipeAddress{} }

type pipeAddress struct{}

func (pipeAddress) Network() string { return "pipe" }

func (pipeAddress) String() string { return "pipe" }
