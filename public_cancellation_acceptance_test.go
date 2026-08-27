package cord_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	moderncsqlite "modernc.org/sqlite"
)

const cancellationAcceptanceHangGuard = 5 * time.Second

type cancellationBarrier struct {
	started chan struct{}
	stopped chan struct{}
	release chan struct{}
	done    chan error
	address string
}

func interruptibleCancellationStep(ctx context.Context, address string) (string, error) {
	return runCancellationStep(ctx, address, false)
}

func nonCooperativeCancellationStep(ctx context.Context, address string) (string, error) {
	return runCancellationStep(ctx, address, true)
}

func runCancellationStep(ctx context.Context, address string, awaitRelease bool) (_ string, err error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return "", fmt.Errorf("connect cancellation callback barrier: %w", err)
	}
	defer func() { err = errors.Join(err, connection.Close()) }()

	if _, err = connection.Write([]byte{1}); err != nil {
		return "", fmt.Errorf("signal cancellation callback start: %w", err)
	}

	<-ctx.Done()

	if _, err = connection.Write([]byte{2}); err != nil {
		return "", errors.Join(ctx.Err(), fmt.Errorf("signal cancellation callback stop: %w", err))
	}

	if !awaitRelease {
		return "", fmt.Errorf("cancellation callback interrupted: %w", ctx.Err())
	}

	if _, err = io.ReadFull(connection, make([]byte, 1)); err != nil {
		return "", errors.Join(ctx.Err(), fmt.Errorf("await cancellation callback release: %w", err))
	}

	return "", fmt.Errorf("cancellation callback released after cancellation: %w", ctx.Err())
}

func newCancellationBarrier(t *testing.T, releaseCallback bool) *cancellationBarrier {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, listener.Close()) })

	barrier := &cancellationBarrier{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan error, 1),
		address: listener.Addr().String(),
	}

	go func() {
		barrier.done <- serveCancellationBarrier(listener, barrier, releaseCallback)
	}()

	return barrier
}

func serveCancellationBarrier(
	listener net.Listener,
	barrier *cancellationBarrier,
	releaseCallback bool,
) (err error) {
	connection, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("accept cancellation callback barrier: %w", err)
	}
	defer func() {
		err = errors.Join(err, connection.Close())
	}()

	if _, err = io.ReadFull(connection, make([]byte, 1)); err != nil {
		return fmt.Errorf("read cancellation callback start: %w", err)
	}

	close(barrier.started)

	if _, err = io.ReadFull(connection, make([]byte, 1)); err != nil {
		return fmt.Errorf("read cancellation callback stop: %w", err)
	}

	close(barrier.stopped)

	if !releaseCallback {
		return nil
	}

	<-barrier.release

	if _, err = connection.Write([]byte{3}); err != nil {
		return fmt.Errorf("release cancellation callback: %w", err)
	}

	return nil
}

func releaseCancellationBarrier(barrier *cancellationBarrier) {
	select {
	case <-barrier.release:
	default:
		close(barrier.release)
	}
}

func awaitCancellationBarrierDone(t *testing.T, barrier *cancellationBarrier) {
	t.Helper()

	select {
	case err := <-barrier.done:
		require.NoError(t, err)
	case <-time.After(cancellationAcceptanceHangGuard):
		t.Fatal("timed out waiting for cancellation callback barrier to finish")
	}
}

func awaitCancellationSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(cancellationAcceptanceHangGuard):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func newCancellationRuntime(t *testing.T, database *sql.DB, options cord.Options) *cord.Cord {
	t.Helper()

	runtime, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)

	return runtime
}

// TestPublicCancelInterruptsCallbackInIndependentRuntime verifies that cancellation
// from another runtime interrupts the active callback.
func TestPublicCancelInterruptsCallbackInIndependentRuntime(t *testing.T) {
	t.Parallel()

	dsn := "file:" + t.TempDir() + "/cross-runtime-cancellation.db" +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	workerDatabase, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, workerDatabase.Close()) })

	controllerDatabase, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, controllerDatabase.Close()) })

	worker := newCancellationRuntime(t, workerDatabase, cord.Options{
		Concurrency:       1,
		PollInterval:      time.Hour,
		LeaseTTL:          time.Second,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	controller := newCancellationRuntime(t, controllerDatabase, cord.Options{
		Concurrency:       1,
		PollInterval:      time.Hour,
		LeaseTTL:          time.Second,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	t.Cleanup(func() {
		assert.NoError(t, controller.Close())
		assert.NoError(t, worker.Close())
	})

	workerFlow := worker.From("cross-runtime-cancellation-callback", interruptibleCancellationStep)
	controllerFlow := controller.From("cross-runtime-cancellation-callback", interruptibleCancellationStep)
	barrier := newCancellationBarrier(t, false)

	runID, err := workerFlow.Submit(t.Context(), barrier.address)
	require.NoError(t, err)
	awaitCancellationSignal(t, barrier.started, "worker callback to start")

	require.NoError(t, controllerFlow.Cancel(t.Context(), runID))
	awaitCancellationSignal(t, barrier.stopped, "independent runtime heartbeat to interrupt callback")
	awaitCancellationBarrierDone(t, barrier)

	report, err := controller.InspectRun(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, cord.RunStateCanceled, report.State)
	assert.Equal(t, cord.ReasonCanceledByRequest, report.Reason)
}

// TestPublicCancelWithBlockedHeartbeatAndBoundedShutdown verifies cancellation
// and bounded shutdown while heartbeat persistence is blocked.
func TestPublicCancelWithBlockedHeartbeatAndBoundedShutdown(t *testing.T) {
	t.Parallel()

	dsn := "file:" + t.TempDir() + "/blocked-heartbeat-cancellation.db" +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	heartbeatStarted := make(chan struct{}, 1)
	releaseHeartbeat := make(chan struct{})
	workerDatabase := openBlockedHeartbeatSQLite(t, dsn, heartbeatStarted, releaseHeartbeat)
	controllerDatabase, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, controllerDatabase.Close()) })

	worker := newCancellationRuntime(t, workerDatabase, cord.Options{
		Concurrency:       1,
		PollInterval:      time.Hour,
		LeaseTTL:          200 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	controller := newCancellationRuntime(t, controllerDatabase, cord.Options{
		Concurrency:       1,
		PollInterval:      time.Hour,
		LeaseTTL:          200 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { assert.NoError(t, controller.Close()) })

	workerFlow := worker.From("blocked-heartbeat-cancellation-shutdown", nonCooperativeCancellationStep)
	controllerFlow := controller.From("blocked-heartbeat-cancellation-shutdown", nonCooperativeCancellationStep)
	barrier := newCancellationBarrier(t, true)
	t.Cleanup(func() {
		releaseCancellationBarrier(barrier)

		select {
		case <-releaseHeartbeat:
		default:
			close(releaseHeartbeat)
		}

		assert.NoError(t, worker.Close())
	})

	runID, err := workerFlow.Submit(t.Context(), barrier.address)
	require.NoError(t, err)
	awaitCancellationSignal(t, barrier.started, "non-cooperative callback to start")
	awaitCancellationSignal(t, heartbeatStarted, "heartbeat storage call to block")

	require.NoError(t, controllerFlow.Cancel(t.Context(), runID))
	awaitCancellationSignal(t, barrier.stopped, "lease safety to cancel callback despite blocked heartbeat")

	shutdownCtx, cancelShutdown := context.WithCancel(t.Context())
	cancelShutdown()

	shutdownResult := make(chan error, 1)
	go func() {
		shutdownResult <- worker.Shutdown(shutdownCtx)
	}()

	select {
	case shutdownErr := <-shutdownResult:
		require.ErrorIs(t, shutdownErr, context.Canceled)
	case <-time.After(cancellationAcceptanceHangGuard):
		t.Fatal("timed out waiting for bounded worker shutdown")
	}

	report, err := controller.InspectRun(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, cord.RunStateCanceled, report.State)
	assert.Equal(t, cord.ReasonCanceledByRequest, report.Reason)

	releaseCancellationBarrier(barrier)
	awaitCancellationBarrierDone(t, barrier)
	close(releaseHeartbeat)
	require.NoError(t, worker.Close())
}

type blockedHeartbeatSQLiteConnector struct {
	driver           driver.Driver
	releaseHeartbeat <-chan struct{}
	heartbeatStarted chan<- struct{}
	dsn              string
}

// Connect opens a SQLite connection that can block heartbeat queries.
func (connector *blockedHeartbeatSQLiteConnector) Connect(_ context.Context) (driver.Conn, error) {
	connection, err := connector.driver.Open(connector.dsn)
	if err != nil {
		return nil, fmt.Errorf("open blocked-heartbeat SQLite connection: %w", err)
	}

	return &blockedHeartbeatSQLiteConnection{
		Conn:             connection,
		heartbeatStarted: connector.heartbeatStarted,
		releaseHeartbeat: connector.releaseHeartbeat,
	}, nil
}

// Driver returns the connector's underlying SQLite driver.
func (connector *blockedHeartbeatSQLiteConnector) Driver() driver.Driver {
	return connector.driver
}

type blockedHeartbeatSQLiteConnection struct {
	driver.Conn
	heartbeatStarted chan<- struct{}
	releaseHeartbeat <-chan struct{}
}

// QueryContext blocks heartbeat queries until the test releases them.
func (connection *blockedHeartbeatSQLiteConnection) QueryContext(
	ctx context.Context,
	query string,
	arguments []driver.NamedValue,
) (driver.Rows, error) {
	queryer, ok := connection.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	if strings.Contains(query, "SET lease_expires_at") {
		select {
		case connection.heartbeatStarted <- struct{}{}:
		default:
		}

		<-connection.releaseHeartbeat
	}

	rows, err := queryer.QueryContext(ctx, query, arguments)
	if err != nil {
		return nil, fmt.Errorf("query blocked-heartbeat SQLite connection: %w", err)
	}

	return rows, nil
}

// PrepareContext delegates statement preparation to the wrapped connection.
func (connection *blockedHeartbeatSQLiteConnection) PrepareContext(
	ctx context.Context,
	query string,
) (driver.Stmt, error) {
	preparer, ok := connection.Conn.(driver.ConnPrepareContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	statement, err := preparer.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("prepare blocked-heartbeat SQLite connection: %w", err)
	}

	return statement, nil
}

// BeginTx delegates transaction creation to the wrapped connection.
func (connection *blockedHeartbeatSQLiteConnection) BeginTx(
	ctx context.Context,
	options driver.TxOptions,
) (driver.Tx, error) {
	beginner, ok := connection.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("blocked-heartbeat SQLite connection does not implement BeginTx")
	}

	return wrapBlockedHeartbeatTransaction(beginner.BeginTx(ctx, options))
}

func wrapBlockedHeartbeatTransaction(transaction driver.Tx, err error) (driver.Tx, error) {
	if err != nil {
		return nil, fmt.Errorf("begin blocked-heartbeat SQLite transaction: %w", err)
	}

	return transaction, nil
}

// ExecContext delegates query execution to the wrapped connection.
func (connection *blockedHeartbeatSQLiteConnection) ExecContext(
	ctx context.Context,
	query string,
	arguments []driver.NamedValue,
) (driver.Result, error) {
	execer, ok := connection.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	result, err := execer.ExecContext(ctx, query, arguments)
	if err != nil {
		return nil, fmt.Errorf("execute blocked-heartbeat SQLite query: %w", err)
	}

	return result, nil
}

// CheckNamedValue delegates argument validation to the wrapped connection.
func (connection *blockedHeartbeatSQLiteConnection) CheckNamedValue(value *driver.NamedValue) error {
	checker, ok := connection.Conn.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}

	if err := checker.CheckNamedValue(value); err != nil {
		return fmt.Errorf("check blocked-heartbeat SQLite argument: %w", err)
	}

	return nil
}

func openBlockedHeartbeatSQLite(
	t *testing.T,
	dsn string,
	heartbeatStarted chan<- struct{},
	releaseHeartbeat <-chan struct{},
) *sql.DB {
	t.Helper()

	database := sql.OpenDB(&blockedHeartbeatSQLiteConnector{
		dsn: dsn, driver: &moderncsqlite.Driver{},
		heartbeatStarted: heartbeatStarted,
		releaseHeartbeat: releaseHeartbeat,
	})
	database.SetMaxOpenConns(4)
	database.SetMaxIdleConns(4)
	require.NoError(t, database.PingContext(t.Context()))
	t.Cleanup(func() { assert.NoError(t, database.Close()) })

	return database
}
