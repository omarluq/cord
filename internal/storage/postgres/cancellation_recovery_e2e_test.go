package postgres_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/storage"
	postgresstore "github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postgresCancellationRecoveryStep(ctx context.Context, directory string) (string, error) {
	if err := os.WriteFile(filepath.Join(directory, "started"), []byte("started"), 0o600); err != nil {
		return "", fmt.Errorf("write PostgreSQL cancellation start marker: %w", err)
	}

	<-ctx.Done()
	cancellationErr := ctx.Err()

	if err := os.WriteFile(filepath.Join(directory, "canceled"), []byte("canceled"), 0o600); err != nil {
		return "", errors.Join(cancellationErr, fmt.Errorf("write PostgreSQL cancellation marker: %w", err))
	}

	return "", fmt.Errorf("PostgreSQL test attempt canceled: %w", cancellationErr)
}

type postCommitAcknowledgementLoss struct {
	err    error
	armed  atomic.Bool
	losses atomic.Int64
}

func (fault *postCommitAcknowledgementLoss) arm() {
	fault.armed.Store(true)
}

type postCommitAcknowledgementLossConnector struct {
	connector driver.Connector
	fault     *postCommitAcknowledgementLoss
}

func (connector postCommitAcknowledgementLossConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connection, err := connector.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect PostgreSQL acknowledgement-loss wrapper: %w", err)
	}

	return &postCommitAcknowledgementLossConn{Conn: connection, fault: connector.fault}, nil
}

func (connector postCommitAcknowledgementLossConnector) Driver() driver.Driver {
	return connector.connector.Driver()
}

type postCommitAcknowledgementLossConn struct {
	driver.Conn
	fault *postCommitAcknowledgementLoss
}

func (connection *postCommitAcknowledgementLossConn) BeginTx(
	ctx context.Context,
	options driver.TxOptions,
) (driver.Tx, error) {
	beginner, ok := connection.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("PostgreSQL acknowledgement-loss connection does not implement BeginTx")
	}

	transaction, err := beginner.BeginTx(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("begin PostgreSQL acknowledgement-loss transaction: %w", err)
	}

	return &postCommitAcknowledgementLossTx{Tx: transaction, fault: connection.fault}, nil
}

type postCommitAcknowledgementLossTx struct {
	driver.Tx
	fault *postCommitAcknowledgementLoss
}

func (transaction *postCommitAcknowledgementLossTx) Commit() error {
	if err := transaction.Tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL acknowledgement-loss transaction: %w", err)
	}

	if transaction.fault.armed.CompareAndSwap(true, false) {
		transaction.fault.losses.Add(1)

		return fmt.Errorf("simulate PostgreSQL post-commit acknowledgement loss: %w", transaction.fault.err)
	}

	return nil
}

func openPostCommitAcknowledgementLossPool(
	t *testing.T,
	dsn string,
	fault *postCommitAcknowledgementLoss,
) *sql.DB {
	t.Helper()

	config, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)

	connector := postCommitAcknowledgementLossConnector{
		connector: stdlib.GetConnector(*config),
		fault:     fault,
	}
	database := sql.OpenDB(connector)
	database.SetMaxOpenConns(4)
	database.SetMaxIdleConns(2)
	require.NoError(t, database.PingContext(t.Context()))
	t.Cleanup(func() { assert.NoError(t, database.Close()) })

	return database
}

func TestPostgresCancellationSurvivesAcknowledgementLossAndRestart(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	setupPool := openPostgres(t, dsn)
	require.NoError(t, postgresstore.Migrate(t.Context(), setupPool))

	fault := &postCommitAcknowledgementLoss{
		err:   errors.New("cancellation commit acknowledgement lost"),
		armed: atomic.Bool{}, losses: atomic.Int64{},
	}
	runtimePool := openPostCommitAcknowledgementLossPool(t, dsn, fault)
	firstRuntime, err := cord.New(t.Context(), runtimePool, cord.Options{
		Concurrency:       1,
		PollInterval:      time.Hour,
		LeaseTTL:          time.Minute,
		HeartbeatInterval: 10 * time.Second,
	})
	require.NoError(t, err)

	stalePool := openPostgresPool(t, dsn)
	staleStore, err := postgresstore.New(stalePool)
	require.NoError(t, err)

	directory := t.TempDir()
	flow := firstRuntime.From("postgres-cancellation-recovery", postgresCancellationRecoveryStep)
	runID, err := flow.Submit(t.Context(), directory)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(directory, "started"))

		return statErr == nil
	}, 10*time.Second, 10*time.Millisecond)

	var staleClaim storage.Claim
	require.NoError(t, stalePool.QueryRowContext(t.Context(), `SELECT node_id, lease_owner,
		lease_generation, lease_expires_at FROM cord_nodes
		WHERE run_id = $1 AND status = 'running'`, runID).Scan(
		&staleClaim.NodeID,
		&staleClaim.Lease.Owner,
		&staleClaim.Lease.Generation,
		&staleClaim.Lease.ExpiresAt,
	))
	staleClaim.RunID = storage.RunID(runID)
	staleClaim.Lease.Remaining = time.Minute

	fault.arm()
	require.NoError(t, flow.Cancel(t.Context(), runID))
	assert.Equal(t, int64(1), fault.losses.Load(), "the committed cancellation response must be lost once")
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(directory, "canceled"))

		return statErr == nil
	}, 10*time.Second, 10*time.Millisecond)

	result, err := staleStore.GetRunResult(t.Context(), staleClaim.RunID)
	require.NoError(t, err)
	require.Equal(t, storage.RunCanceled, result.Status)

	require.NoError(t, firstRuntime.Close())
	require.NoError(t, runtimePool.Close())

	assertPostgresCancellationAfterRestart(t, dsn, runID, staleStore, &staleClaim)
}

func assertPostgresCancellationAfterRestart(
	t *testing.T,
	dsn string,
	runID cord.RunID,
	staleStore *postgresstore.Store,
	staleClaim *storage.Claim,
) {
	t.Helper()

	freshPool := openPostgresPool(t, dsn)
	freshStore, err := postgresstore.New(freshPool)
	require.NoError(t, err)
	freshRuntime, err := cord.New(t.Context(), freshPool, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, freshRuntime.Close()) })

	freshFlow := freshRuntime.From("postgres-cancellation-recovery", postgresCancellationRecoveryStep)
	_, err = freshFlow.Get(t.Context(), runID)
	require.ErrorIs(t, err, cord.ErrRunCanceled)

	assertFreshPostgresCancellationObservations(t, freshRuntime, runID)
	assertPostgresCancellationFencesStaleWork(t, freshStore, staleStore, staleClaim)
}

func assertFreshPostgresCancellationObservations(t *testing.T, runtime *cord.Cord, runID cord.RunID) {
	t.Helper()

	report, err := runtime.InspectRun(t.Context(), runID)
	require.NoError(t, err)
	assert.Equal(t, cord.RunStateCanceled, report.State)
	assert.Equal(t, cord.ReasonCanceledByRequest, report.Reason)
	assert.Equal(t, cord.NodeStateCounts{Canceled: 1}, report.NodeCounts)
	require.NotNil(t, report.FinishedAt)

	page, err := runtime.ListRunNodes(t.Context(), runID, cord.NodeQuery{})
	require.NoError(t, err)
	require.Len(t, page.Nodes, 1)
	assert.Equal(t, cord.NodeStateCanceled, page.Nodes[0].State)
	assert.Equal(t, cord.ReasonCanceledByRequest, page.Nodes[0].Reason)
	assert.Nil(t, page.Nodes[0].CurrentLease)
}

func assertPostgresCancellationFencesStaleWork(
	t *testing.T,
	freshStore, staleStore *postgresstore.Store,
	staleClaim *storage.Claim,
) {
	t.Helper()

	recovered, err := freshStore.RecoverExpiredLeases(t.Context())
	require.NoError(t, err)
	assert.Zero(t, recovered)
	claim, claimed, err := freshStore.ClaimReadyNodeForFunctions(
		t.Context(), "restarted-worker", time.Minute, postgresRegistrations(),
	)
	require.NoError(t, err)
	assert.False(t, claimed)
	assert.Nil(t, claim)

	accepted, err := staleStore.CompleteNode(
		t.Context(), staleClaim.RunID, staleClaim.NodeID, staleClaim.Lease, []byte(`"late"`),
	)
	require.NoError(t, err)
	assert.False(t, accepted)
	accepted, remaining, err := staleStore.HeartbeatNode(
		t.Context(), staleClaim.RunID, staleClaim.NodeID, staleClaim.Lease, time.Minute,
	)
	require.NoError(t, err)
	assert.False(t, accepted)
	assert.Zero(t, remaining)

	retained, err := freshStore.GetRunResult(t.Context(), staleClaim.RunID)
	require.NoError(t, err)
	assert.Equal(t, storage.RunCanceled, retained.Status)
	retainedReport, err := freshStore.InspectRun(t.Context(), staleClaim.RunID)
	require.NoError(t, err)
	assert.Equal(t, storage.RunCanceled, retainedReport.State)
	assert.Equal(t, storage.ReasonCanceledByRequest, retainedReport.Reason)
	assert.Equal(t, storage.NodeStateCounts{
		Pending: 0, Ready: 0, Running: 0, RetryWait: 0,
		Completed: 0, Failed: 0, Canceled: 1,
	}, retainedReport.NodeCounts)
}
