package postgres_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/conformance"
	postgresstore "github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	postgresImage    = "postgres:16.4-alpine"
	fixtureTimeout   = 2 * time.Minute
	operationTimeout = 30 * time.Second
)

func startPostgres(t *testing.T) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), fixtureTimeout)
	defer cancel()

	dsn := os.Getenv("CORD_POSTGRES_DSN")
	if dsn == "" {
		container, err := postgrescontainer.Run(
			ctx,
			postgresImage,
			postgrescontainer.WithDatabase("cord"),
			postgrescontainer.WithUsername("cord"),
			postgrescontainer.WithPassword("cord"),
			postgrescontainer.BasicWaitStrategies(),
		)
		require.NoError(t, err, "start PostgreSQL test container")

		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), operationTimeout)
			defer cleanupCancel()

			assert.NoError(t, container.Terminate(cleanupCtx), "terminate PostgreSQL test container")
		})

		dsn, err = container.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err, "resolve PostgreSQL test connection")
	}

	return isolatePostgresSchema(t, dsn)
}

func isolatePostgresSchema(t *testing.T, dsn string) string {
	t.Helper()

	schema := "cord_test_" + strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()

		_, dropErr := admin.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		assert.NoError(t, dropErr, "drop isolated PostgreSQL schema")
		assert.NoError(t, admin.Close(), "close PostgreSQL schema administrator")
	})

	if parsed, parseErr := url.Parse(dsn); parseErr == nil && parsed.Scheme != "" {
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()

		return parsed.String()
	}

	return dsn + " search_path=" + schema
}

func openPostgres(tb testing.TB, dsn string) *sql.DB {
	tb.Helper()

	database, err := sql.Open("pgx", dsn)
	if err != nil {
		tb.Fatalf("open PostgreSQL: %v", err)
	}

	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(8)

	if err = database.PingContext(context.Background()); err != nil {
		closeErr := database.Close()

		tb.Fatalf("ping PostgreSQL: %v (close after failure: %v)", err, closeErr)
	}

	const resetQuery = `DROP TABLE IF EXISTS cord_edges, cord_nodes, cord_runs, cord_schema_migrations CASCADE`
	if _, err = database.ExecContext(context.Background(), resetQuery); err != nil {
		closeErr := database.Close()

		tb.Fatalf("reset PostgreSQL fixture: %v (close after failure: %v)", err, closeErr)
	}

	tb.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			tb.Errorf("close PostgreSQL: %v", closeErr)
		}
	})

	return database
}

func openPostgresPool(tb testing.TB, dsn string) *sql.DB {
	tb.Helper()

	database, err := sql.Open("pgx", dsn)
	require.NoError(tb, err)
	database.SetMaxOpenConns(16)
	database.SetMaxIdleConns(16)
	require.NoError(tb, database.PingContext(context.Background()))
	tb.Cleanup(func() { assert.NoError(tb, database.Close()) })

	return database
}

func postgresHarness(dsn string) conformance.Harness {
	openFixture := func(tb testing.TB, _ string) *sql.DB {
		tb.Helper()

		return openPostgres(tb, dsn)
	}

	return conformance.Harness{
		Open:    openFixture,
		Migrate: postgresstore.Migrate,
		NewBackend: func(database *sql.DB) (storage.Backend, error) {
			return postgresstore.New(database)
		},
		ExpireLease: func(
			ctx context.Context,
			database *sql.DB,
			runID storage.RunID,
			nodeID storage.NodeID,
		) error {
			const query = `UPDATE cord_nodes SET lease_expires_at = TIMESTAMPTZ '2000-01-01 00:00:00+00'
				WHERE run_id = $1 AND node_id = $2`
			if _, err := database.ExecContext(ctx, query, runID, nodeID); err != nil {
				return fmt.Errorf("expire PostgreSQL lease: %w", err)
			}

			return nil
		},
		CancelRun: func(ctx context.Context, backend storage.Backend, runID storage.RunID) (bool, error) {
			store, ok := backend.(*postgresstore.Store)
			if !ok {
				return false, fmt.Errorf("cancel PostgreSQL run: backend type %T", backend)
			}

			return store.CancelRun(ctx, runID)
		},
		DeleteRun: func(ctx context.Context, database *sql.DB, runID storage.RunID) error {
			if _, err := database.ExecContext(ctx, "DELETE FROM cord_runs WHERE id = $1", runID); err != nil {
				return fmt.Errorf("delete PostgreSQL run: %w", err)
			}

			return nil
		},
		CountRunRows: func(
			ctx context.Context,
			database *sql.DB,
			runID storage.RunID,
		) (int, int, error) {
			var nodes, edges int

			if err := database.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM cord_nodes WHERE run_id = $1", runID).Scan(&nodes); err != nil {
				return 0, 0, fmt.Errorf("count PostgreSQL node rows: %w", err)
			}

			if err := database.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM cord_edges WHERE run_id = $1", runID).Scan(&edges); err != nil {
				return 0, 0, fmt.Errorf("count PostgreSQL edge rows: %w", err)
			}

			return nodes, edges, nil
		},
	}
}

func TestPostgresConformance(t *testing.T) {
	t.Parallel()

	conformance.Run(t, postgresHarness(startPostgres(t)))
}

func TestSchemaVerification(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	tests := []struct {
		name       string
		alteration string
	}{
		{"column type", `ALTER TABLE cord_runs ALTER COLUMN status TYPE VARCHAR(20)`},
		{"column nullability", `ALTER TABLE cord_nodes ALTER COLUMN function_key DROP NOT NULL`},
		{"column default", `ALTER TABLE cord_edges ALTER COLUMN parent_order SET DEFAULT 1`},
		{"primary key order", `ALTER TABLE cord_edges DROP CONSTRAINT cord_edges_pkey,
			ADD PRIMARY KEY (parent_node_id, run_id, child_node_id)`},
		{"named index columns", `DROP INDEX cord_nodes_run_status_idx;
			CREATE INDEX cord_nodes_run_status_idx ON cord_nodes(status, run_id)`},
		{"partial index", `DROP INDEX cord_nodes_lease_expires_at_idx;
			CREATE INDEX cord_nodes_lease_expires_at_idx ON cord_nodes(lease_expires_at)
			WHERE lease_expires_at IS NOT NULL`},
		{"parent-output index columns", `DROP INDEX cord_edges_run_child_parent_order_idx;
			CREATE INDEX cord_edges_run_child_parent_order_idx
			ON cord_edges(child_node_id, run_id, parent_order)`},
		{"foreign key delete action", `ALTER TABLE cord_nodes DROP CONSTRAINT cord_nodes_run_id_fkey;
			ALTER TABLE cord_nodes ADD FOREIGN KEY (run_id) REFERENCES cord_runs(id)`},
	}

	for _, test := range tests {
		database := openPostgres(t, dsn)
		require.NoError(t, postgresstore.Migrate(t.Context(), database), test.name)

		_, err := database.ExecContext(t.Context(), test.alteration)
		require.NoError(t, err, test.name)

		err = postgresstore.Verify(t.Context(), database)
		require.ErrorIs(t, err, storage.ErrSchemaOutdated, test.name)
	}

	database := openPostgres(t, dsn)
	require.NoError(t, postgresstore.Migrate(t.Context(), database))

	_, err := database.ExecContext(t.Context(), `ALTER TABLE cord_runs ADD COLUMN extension_data JSONB;
		CREATE INDEX extension_index ON cord_runs(workflow_name)`)
	require.NoError(t, err)
	require.NoError(t, postgresstore.Verify(t.Context(), database))
}

func TestMigrateAddsParentOutputIndexToVersionOneSchema(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	require.NoError(t, postgresstore.Migrate(t.Context(), database))
	_, err := database.ExecContext(t.Context(), `DROP INDEX cord_edges_run_child_parent_order_idx;
		DELETE FROM cord_schema_migrations WHERE version_id = 2`)
	require.NoError(t, err)

	require.NoError(t, postgresstore.Migrate(t.Context(), database))
	require.NoError(t, postgresstore.Verify(t.Context(), database))

	var indexExists bool

	err = database.QueryRowContext(t.Context(), `SELECT to_regclass(
		format('%I.%I', current_schema(), 'cord_edges_run_child_parent_order_idx')) IS NOT NULL`).Scan(&indexExists)
	require.NoError(t, err)
	assert.True(t, indexExists)
}

func TestMigratePreflightsNewerSchema(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	_, err := database.ExecContext(t.Context(), `CREATE TABLE cord_schema_migrations (
		id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		version_id BIGINT NOT NULL,
		is_applied BOOLEAN NOT NULL,
		tstamp TIMESTAMP NOT NULL DEFAULT now()
	); INSERT INTO cord_schema_migrations(version_id, is_applied) VALUES (3, true)`)
	require.NoError(t, err)

	err = postgresstore.Migrate(t.Context(), database)
	require.ErrorIs(t, err, storage.ErrSchemaNewer)

	var applicationTables int

	err = database.QueryRowContext(t.Context(), `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name IN ('cord_runs', 'cord_nodes', 'cord_edges')`).
		Scan(&applicationTables)
	require.NoError(t, err)
	assert.Zero(t, applicationTables, "migration DDL must not run for a newer schema")
}

func TestConcurrentMigrations(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))

	const constructors = 20

	results := make(chan error, constructors)

	var group sync.WaitGroup
	for range constructors {
		group.Go(func() {
			cordRuntime, err := cord.New(t.Context(), database)
			if err == nil {
				err = cordRuntime.Close()
			}

			results <- err
		})
	}

	group.Wait()
	close(results)

	for resultErr := range results {
		require.NoError(t, resultErr)
	}

	var migrations int

	const query = "SELECT count(*) FROM cord_schema_migrations WHERE version_id = 1"
	require.NoError(t, database.QueryRowContext(t.Context(), query).Scan(&migrations))
	assert.Equal(t, 1, migrations)
}

func postgresAddOne(_ context.Context, value int) (int, error)     { return value + 1, nil }
func postgresDouble(_ context.Context, value int) (int, error)     { return value * 2, nil }
func postgresJoin(_ context.Context, left, right int) (int, error) { return left + right, nil }

func postgresRetryUntilFileExists(_ context.Context, path string) (string, error) {
	if _, err := os.Stat(path); err != nil {
		return path, fmt.Errorf("wait for resume marker: %w", err)
	}

	return path, nil
}

func TestCordNewPublicWorkflowsAndCallerOwnership(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	cordRuntime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)

	linear, err := cordRuntime.From("postgres-linear", postgresAddOne).Then(postgresDouble).Run(t.Context(), 4)
	require.NoError(t, err)
	assert.Equal(t, 10, linear)

	root := cordRuntime.From("postgres-join", postgresAddOne)
	joined, err := cord.Join(root.Then(postgresDouble), root.Then(postgresAddOne)).
		Then(postgresJoin).
		Run(t.Context(), 3)
	require.NoError(t, err)
	assert.Equal(t, 13, joined)

	require.NoError(t, cordRuntime.Close())
	require.NoError(t, database.PingContext(t.Context()), "Cord must not close its caller-owned database")
}

type wrappedPGXConnector struct {
	connector driver.Connector
}

func (connector wrappedPGXConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connection, err := connector.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect wrapped pgx driver: %w", err)
	}

	return connection, nil
}

func (connector wrappedPGXConnector) Driver() driver.Driver {
	return wrappedPGXDriver{driver: connector.connector.Driver()}
}

type wrappedPGXDriver struct {
	driver driver.Driver
}

func (wrapped wrappedPGXDriver) Open(name string) (driver.Conn, error) {
	connection, err := wrapped.driver.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open wrapped pgx driver: %w", err)
	}

	return connection, nil
}

func TestCordNewDetectsPostgresThroughWrappedPGXConnector(t *testing.T) {
	t.Parallel()

	config, err := pgx.ParseConfig(startPostgres(t))
	require.NoError(t, err)

	pgxConnector := stdlib.GetConnector(*config)
	database := sql.OpenDB(wrappedPGXConnector{connector: pgxConnector})
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(8)
	t.Cleanup(func() { assert.NoError(t, database.Close()) })

	require.NotEqual(t, reflect.TypeOf(pgxConnector.Driver()), reflect.TypeOf(database.Driver()))

	cordRuntime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cordRuntime.Close()) })

	result, err := cordRuntime.From("postgres-wrapped-pgx", postgresAddOne).Run(t.Context(), 4)
	require.NoError(t, err)
	assert.Equal(t, 5, result)
}

func TestMultipleRuntimesClaimEachRunOnce(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	options := cord.Options{Concurrency: 16, PollInterval: time.Millisecond}
	first, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)
	second, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, first.Close())
		assert.NoError(t, second.Close())
	})

	firstFlow := first.From("postgres-shared-claims", postgresAddOne)
	secondFlow := second.From("postgres-shared-claims", postgresAddOne)

	const runs = 100

	runErrors := make(chan error, runs)

	var group sync.WaitGroup
	for index := range runs {
		group.Go(func() {
			flow := firstFlow
			if index%2 != 0 {
				flow = secondFlow
			}

			result, runErr := flow.Run(t.Context(), index)
			if runErr == nil && result != index+1 {
				runErr = fmt.Errorf("result = %d, want %d", result, index+1)
			}

			runErrors <- runErr
		})
	}

	group.Wait()
	close(runErrors)

	for runErr := range runErrors {
		require.NoError(t, runErr)
	}

	var duplicateAttempts int

	const duplicateQuery = `SELECT count(*) FROM cord_nodes n JOIN cord_runs r ON r.id=n.run_id
		WHERE r.workflow_name=$1 AND n.attempt <> 1`
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		duplicateQuery,
		"postgres-shared-claims",
	).Scan(&duplicateAttempts))
	assert.Zero(t, duplicateAttempts)
}

func TestConcurrentClaimersAcrossPoolsClaimEachNodeOnce(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	database := openPostgres(t, dsn)
	require.NoError(t, postgresstore.Migrate(t.Context(), database))

	const (
		claimers = 100
		pools    = 4
	)

	stores := make([]*postgresstore.Store, pools)
	for index := range stores {
		store, err := postgresstore.New(openPostgresPool(t, dsn))
		require.NoError(t, err)

		stores[index] = store
	}

	for index := range claimers {
		plan := postgresReadyPlan(storage.RunID(fmt.Sprintf("concurrent-claim-%03d", index)), time.Now().UTC())
		require.NoError(t, stores[0].CreateRun(t.Context(), &plan))
	}

	start := make(chan struct{})
	claims := make(chan *storage.Claim, claimers)
	errs := make(chan error, claimers)

	var group sync.WaitGroup
	for index := range claimers {
		group.Go(func() {
			<-start

			claim, claimed, err := stores[index%pools].ClaimReadyNodeForFunctions(
				t.Context(), fmt.Sprintf("claimer-%03d", index), time.Minute, postgresRegistrations())
			if err == nil && !claimed {
				err = fmt.Errorf("claimer %d did not claim a node", index)
			}

			if err != nil {
				errs <- err

				return
			}

			claims <- claim
		})
	}

	close(start)
	group.Wait()
	close(claims)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	unique := make(map[string]struct{}, claimers)

	for claim := range claims {
		key := string(claim.RunID) + "/" + string(claim.NodeID)
		_, duplicate := unique[key]
		assert.False(t, duplicate, "duplicate claim %s", key)
		unique[key] = struct{}{}
	}

	assert.Len(t, unique, claimers)
}

func TestClaimSkipsLockedFirstCandidate(t *testing.T) {
	t.Parallel()

	dsn := startPostgres(t)
	database := openPostgres(t, dsn)
	require.NoError(t, postgresstore.Migrate(t.Context(), database))
	store, err := postgresstore.New(openPostgresPool(t, dsn))
	require.NoError(t, err)

	first := postgresReadyPlan("locked-first", time.Now().UTC().Add(-time.Second))
	second := postgresReadyPlan("unlocked-second", time.Now().UTC())

	require.NoError(t, store.CreateRun(t.Context(), &first))
	require.NoError(t, store.CreateRun(t.Context(), &second))

	transaction, err := database.BeginTx(t.Context(), nil)

	require.NoError(t, err)
	defer func() { assert.NoError(t, transaction.Rollback()) }()

	var locked storage.RunID

	err = transaction.QueryRowContext(t.Context(), `SELECT run_id FROM cord_nodes
		WHERE status='ready' ORDER BY available_at, run_id, node_id LIMIT 1 FOR UPDATE`).Scan(&locked)
	require.NoError(t, err)
	require.Equal(t, first.Run.ID, locked)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	claim, claimed, err := store.ClaimReadyNodeForFunctions(
		ctx, "skip-locked-worker", time.Minute, postgresRegistrations(),
	)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotNil(t, claim)
	assert.Equal(t, second.Run.ID, claim.RunID)
}

func TestStaleLeasesRejectTransitionsWithoutMutation(t *testing.T) {
	t.Parallel()

	type operation struct {
		run  func(context.Context, *postgresstore.Store, *storage.Claim) (bool, error)
		name string
	}

	operations := []operation{
		{run: func(ctx context.Context, store *postgresstore.Store, claim *storage.Claim) (bool, error) {
			return store.CompleteNode(ctx, claim.RunID, claim.NodeID, claim.Lease, []byte("complete"))
		}, name: "complete"},
		{run: func(ctx context.Context, store *postgresstore.Store, claim *storage.Claim) (bool, error) {
			return store.RetryNode(ctx, claim.RunID, claim.NodeID, claim.Lease, []byte("retry"), time.Minute)
		}, name: "retry"},
		{run: func(ctx context.Context, store *postgresstore.Store, claim *storage.Claim) (bool, error) {
			return store.FailNode(ctx, claim.RunID, claim.NodeID, claim.Lease, []byte("fail"))
		}, name: "fail"},
		{run: func(ctx context.Context, store *postgresstore.Store, claim *storage.Claim) (bool, error) {
			accepted, _, err := store.HeartbeatNode(ctx, claim.RunID, claim.NodeID, claim.Lease, time.Minute)
			if err != nil {
				return false, fmt.Errorf("heartbeat stale lease: %w", err)
			}

			return accepted, nil
		}, name: "heartbeat"},
	}
	fences := []struct {
		mutate func(context.Context, *sql.DB, *storage.Claim) error
		name   string
	}{
		{mutate: func(_ context.Context, _ *sql.DB, claim *storage.Claim) error {
			claim.Lease.Owner = "stale-owner"

			return nil
		}, name: "owner"},
		{mutate: func(_ context.Context, _ *sql.DB, claim *storage.Claim) error {
			claim.Lease.Generation--

			return nil
		}, name: "generation"},
		{mutate: func(ctx context.Context, database *sql.DB, claim *storage.Claim) error {
			const query = `UPDATE cord_nodes
				SET lease_expires_at=TIMESTAMPTZ '2000-01-01 00:00:00+00'
				WHERE run_id=$1 AND node_id=$2`
			if _, err := database.ExecContext(ctx, query, claim.RunID, claim.NodeID); err != nil {
				return fmt.Errorf("expire lease: %w", err)
			}

			return nil
		}, name: "expired"},
	}

	dsn := startPostgres(t)
	database := openPostgres(t, dsn)
	require.NoError(t, postgresstore.Migrate(t.Context(), database))
	store, err := postgresstore.New(database)
	require.NoError(t, err)

	for _, operation := range operations {
		for _, fence := range fences {
			runID := storage.RunID("fence-" + operation.name + "-" + fence.name)
			plan := postgresReadyPlan(runID, time.Now().UTC())
			require.NoError(t, store.CreateRun(t.Context(), &plan), operation.name+"/stale_"+fence.name)
			claim, claimed, claimErr := store.ClaimReadyNodeForFunctions(
				t.Context(), "current-owner", time.Minute, postgresRegistrations(),
			)
			require.NoError(t, claimErr, operation.name+"/stale_"+fence.name)
			require.True(t, claimed, operation.name+"/stale_"+fence.name)
			require.Equal(t, runID, claim.RunID, operation.name+"/stale_"+fence.name)
			require.NoError(t, fence.mutate(t.Context(), database, claim), operation.name+"/stale_"+fence.name)
			before := postgresDurableState(t, database, runID, claim.NodeID)
			accepted, operationErr := operation.run(t.Context(), store, claim)
			require.NoError(t, operationErr, operation.name+"/stale_"+fence.name)
			assert.False(t, accepted, operation.name+"/stale_"+fence.name)
			assert.Equal(
				t, before, postgresDurableState(t, database, runID, claim.NodeID),
				operation.name+"/stale_"+fence.name,
			)
		}
	}
}

type postgresState struct {
	nodeStatus, runStatus                    string
	leaseOwner, leaseExpiry, output, failure sql.NullString
	generation, attempt                      int64
}

func postgresDurableState(t *testing.T, database *sql.DB, runID storage.RunID, nodeID storage.NodeID) postgresState {
	t.Helper()

	var state postgresState

	err := database.QueryRowContext(t.Context(), `SELECT n.status, r.status, n.lease_owner,
		n.lease_expires_at::text, encode(n.output_payload, 'hex'), encode(n.error_payload, 'hex'),
		n.lease_generation, n.attempt FROM cord_nodes n JOIN cord_runs r ON r.id=n.run_id
		WHERE n.run_id=$1 AND n.node_id=$2`, runID, nodeID).Scan(
		&state.nodeStatus, &state.runStatus, &state.leaseOwner, &state.leaseExpiry,
		&state.output, &state.failure, &state.generation, &state.attempt)
	require.NoError(t, err)

	return state
}

const postgresTestNode storage.NodeID = "node"

func postgresReadyPlan(runID storage.RunID, availableAt time.Time) storage.RunPlan {
	return storage.RunPlan{
		Run: storage.Run{
			CreatedAt: availableAt, UpdatedAt: availableAt, CompletedAt: nil, ID: runID,
			WorkflowName: "postgres-concurrency", DefinitionHash: "definition", TerminalNodeID: postgresTestNode,
			Status: storage.RunRunning, Input: []byte("input"), Output: nil, Error: nil, MaxAttempts: 3,
			RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Second, RetryPolicyVersion: 1,
		},
		Nodes: []storage.Node{{
			AvailableAt: availableAt, CompletedAt: nil, StartedAt: nil, SignatureHash: "signature",
			RunID: runID, ID: postgresTestNode, FunctionKey: "postgres.test", Status: storage.NodeReady,
			Lease: storage.Lease{}, Error: nil, Output: nil, RemainingDeps: 0, Attempt: 0,
		}},
		Edges: nil,
	}
}

func postgresRegistrations() []storage.FunctionRegistration {
	return []storage.FunctionRegistration{{Key: "postgres.test", Signature: "signature"}}
}

func TestTerminalTransitionsSerializeOnRun(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	require.NoError(t, postgresstore.Migrate(t.Context(), database))
	store, err := postgresstore.New(database)
	require.NoError(t, err)

	complete := func(ctx context.Context, claim *storage.Claim) (bool, error) {
		accepted, transitionErr := store.CompleteNode(
			ctx, claim.RunID, claim.NodeID, claim.Lease, []byte(`"done"`),
		)
		if transitionErr != nil {
			return false, fmt.Errorf("complete terminal node: %w", transitionErr)
		}

		return accepted, nil
	}
	fail := func(ctx context.Context, claim *storage.Claim) (bool, error) {
		accepted, transitionErr := store.FailNode(
			ctx, claim.RunID, claim.NodeID, claim.Lease, []byte(claim.NodeID),
		)
		if transitionErr != nil {
			return false, fmt.Errorf("fail terminal node: %w", transitionErr)
		}

		return accepted, nil
	}

	runTerminalRace(t, database, store, "terminal-completion-race", complete, storage.RunCompleted)
	runTerminalRace(t, database, store, "concurrent-failures", fail, storage.RunFailed)
}

type terminalTransition func(context.Context, *storage.Claim) (bool, error)

func runTerminalRace(
	t *testing.T,
	database *sql.DB,
	store *postgresstore.Store,
	runID storage.RunID,
	terminalTransition terminalTransition,
	completionAllowed storage.RunStatus,
) {
	t.Helper()

	plan := terminalRacePlan(runID)
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	terminal := claimPostgresNode(t, store, "terminal-worker", "terminal-key", "terminal-signature")
	sibling := claimPostgresNode(t, store, "sibling-worker", "sibling-key", "sibling-signature")

	type outcome struct {
		err      error
		accepted bool
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, 2)

	go func() {
		<-start

		accepted, transitionErr := terminalTransition(t.Context(), terminal)
		outcomes <- outcome{err: transitionErr, accepted: accepted}
	}()

	go func() {
		<-start

		accepted, transitionErr := store.FailNode(
			t.Context(), sibling.RunID, sibling.NodeID, sibling.Lease, []byte("sibling"),
		)
		outcomes <- outcome{err: transitionErr, accepted: accepted}
	}()

	close(start)

	accepted := 0

	for range 2 {
		result := <-outcomes
		require.NoError(t, result.err)

		if result.accepted {
			accepted++
		}
	}

	assert.Equal(t, 1, accepted, "exactly one terminal outcome must win")
	assertTerminalRaceState(t, database, runID, completionAllowed)
}

func terminalRacePlan(runID storage.RunID) storage.RunPlan {
	now := time.Now().UTC().Add(-time.Second)

	return storage.RunPlan{
		Edges: nil,
		Run: storage.Run{
			CreatedAt: now, UpdatedAt: now, CompletedAt: nil,
			ID: runID, WorkflowName: string(runID), DefinitionHash: "definition",
			TerminalNodeID: "terminal", Status: storage.RunRunning,
			Input: []byte(`null`), Output: nil, Error: nil,
			MaxAttempts: 1, RetryBaseDelay: time.Millisecond,
			RetryMaxDelay: time.Second, RetryPolicyVersion: 1,
		},
		Nodes: []storage.Node{
			terminalRaceNode(runID, "terminal", "terminal-key", "terminal-signature", now),
			terminalRaceNode(runID, "sibling", "sibling-key", "sibling-signature", now),
		},
	}
}

func terminalRaceNode(
	runID storage.RunID,
	nodeID storage.NodeID,
	key, signature string,
	availableAt time.Time,
) storage.Node {
	return storage.Node{
		AvailableAt: availableAt, CompletedAt: nil, StartedAt: nil,
		SignatureHash: signature, RunID: runID, ID: nodeID, FunctionKey: key,
		Status: storage.NodeReady, Lease: storage.Lease{}, Error: nil, Output: nil,
		RemainingDeps: 0, Attempt: 0,
	}
}

func claimPostgresNode(
	t *testing.T,
	store *postgresstore.Store,
	owner, key, signature string,
) *storage.Claim {
	t.Helper()

	claim, claimed, err := store.ClaimReadyNodeForFunctions(
		t.Context(), owner, time.Minute,
		[]storage.FunctionRegistration{{Key: key, Signature: signature}},
	)
	require.NoError(t, err)
	require.True(t, claimed)

	return claim
}

func assertTerminalRaceState(
	t *testing.T,
	database *sql.DB,
	runID storage.RunID,
	completionAllowed storage.RunStatus,
) {
	t.Helper()

	var runStatus storage.RunStatus
	require.NoError(t, database.QueryRowContext(
		t.Context(), `SELECT status FROM cord_runs WHERE id = $1`, runID,
	).Scan(&runStatus))

	if completionAllowed == storage.RunCompleted {
		assert.Contains(t, []storage.RunStatus{storage.RunCompleted, storage.RunFailed}, runStatus)
	} else {
		assert.Equal(t, storage.RunFailed, runStatus)
	}

	rows, err := database.QueryContext(
		t.Context(), `SELECT status FROM cord_nodes WHERE run_id = $1 ORDER BY node_id`, runID,
	)
	require.NoError(t, err)

	defer func() { require.NoError(t, rows.Close()) }()

	statuses := make([]storage.NodeStatus, 0, 2)

	for rows.Next() {
		var status storage.NodeStatus
		require.NoError(t, rows.Scan(&status))
		statuses = append(statuses, status)
	}

	require.NoError(t, rows.Err())
	require.Len(t, statuses, 2)

	if runStatus == storage.RunCompleted {
		assert.ElementsMatch(t, []storage.NodeStatus{storage.NodeCanceled, storage.NodeCompleted}, statuses)
	} else {
		assert.ElementsMatch(t, []storage.NodeStatus{storage.NodeCanceled, storage.NodeFailed}, statuses)
	}
}

func TestReopenRegistersAndResumesPersistedRetry(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	options := cord.Options{
		PollInterval:   time.Millisecond,
		MaxAttempts:    3,
		RetryBaseDelay: time.Hour,
		RetryMaxDelay:  time.Hour,
	}
	first, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)

	marker := t.TempDir() + "/resume-ready"
	done := make(chan error, 1)

	go func() {
		_, runErr := first.From("postgres-reopen", postgresRetryUntilFileExists).Run(t.Context(), marker)
		done <- runErr
	}()

	const nodeStatusQuery = `SELECT n.status FROM cord_nodes n JOIN cord_runs r ON r.id=n.run_id
		WHERE r.workflow_name=$1`

	require.Eventually(t, func() bool {
		var status string

		queryErr := database.QueryRowContext(
			t.Context(),
			nodeStatusQuery,
			"postgres-reopen",
		).Scan(&status)

		return queryErr == nil && status == "retry_wait"
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, first.Close())
	require.ErrorContains(t, <-done, "runtime closed")

	require.NoError(t, os.WriteFile(marker, []byte("ready"), 0o600))

	const promoteQuery = `UPDATE cord_nodes SET available_at=clock_timestamp()-interval '1 second'
		WHERE status='retry_wait'`

	_, err = database.ExecContext(t.Context(), promoteQuery)
	require.NoError(t, err)
	second, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })
	second.From("postgres-reopen", postgresRetryUntilFileExists)

	const runStatusQuery = `SELECT status FROM cord_runs WHERE workflow_name=$1`

	require.Eventually(t, func() bool {
		var status string

		queryErr := database.QueryRowContext(
			t.Context(),
			runStatusQuery,
			"postgres-reopen",
		).Scan(&status)

		return queryErr == nil && status == "completed"
	}, 10*time.Second, 10*time.Millisecond)
}
