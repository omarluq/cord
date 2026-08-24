package cord_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func increment(_ context.Context, value int) (int, error) {
	return value + 1, nil
}

func double(_ context.Context, value int) (int, error) {
	return value * 2, nil
}

const (
	heartbeatIntervalError = "heartbeat interval"
	leaseTTLError          = "lease TTL"
	maximumDelayError      = "maximum delay"
)

func TestNew_OptionsValidatesSchedulerSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, error string
		options     cord.Options
	}{
		{name: "negative concurrency", options: cord.Options{Concurrency: -1}, error: "concurrency"},
		{name: "negative poll interval", options: cord.Options{PollInterval: -1}, error: "poll interval"},
		{name: "negative lease TTL", options: cord.Options{LeaseTTL: -1}, error: leaseTTLError},
		{
			name: "sub-millisecond lease TTL", options: cord.Options{LeaseTTL: time.Millisecond - 1},
			error: leaseTTLError,
		},
		{name: "one-millisecond lease TTL", options: cord.Options{LeaseTTL: time.Millisecond}, error: leaseTTLError},
		{
			name: "two-millisecond lease TTL", options: cord.Options{LeaseTTL: 2 * time.Millisecond},
			error: leaseTTLError,
		},
		{name: "negative heartbeat", options: cord.Options{HeartbeatInterval: -1}, error: heartbeatIntervalError},
		{name: "sub-millisecond heartbeat", options: cord.Options{
			LeaseTTL: time.Second, HeartbeatInterval: time.Millisecond - 1,
		}, error: heartbeatIntervalError},
		{name: "heartbeat equals half lease", options: cord.Options{
			LeaseTTL: time.Second, HeartbeatInterval: 500 * time.Millisecond,
		}, error: heartbeatIntervalError},
		{name: "heartbeat exceeds half lease", options: cord.Options{
			LeaseTTL: time.Second, HeartbeatInterval: 501 * time.Millisecond,
		}, error: heartbeatIntervalError},
		{name: "heartbeat equals lease", options: cord.Options{
			LeaseTTL: time.Second, HeartbeatInterval: time.Second,
		}, error: heartbeatIntervalError},
		{name: "negative max attempts", options: cord.Options{MaxAttempts: -1}, error: "maximum attempts"},
		{name: "negative retry base delay", options: cord.Options{RetryBaseDelay: -1}, error: "base delay"},
		{name: "negative retry maximum delay", options: cord.Options{RetryMaxDelay: -1}, error: maximumDelayError},
		{name: "retry base exceeds default maximum", options: cord.Options{
			RetryBaseDelay: 31 * time.Second,
		}, error: maximumDelayError},
		{name: "retry maximum precedes default base", options: cord.Options{
			RetryMaxDelay: time.Millisecond,
		}, error: maximumDelayError},
		{name: "retry maximum precedes explicit base", options: cord.Options{
			RetryBaseDelay: time.Second, RetryMaxDelay: time.Millisecond,
		}, error: maximumDelayError},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runtime, err := cord.New(t.Context(), openSQLite(t), testCase.options)

			assert.Nil(t, runtime)
			require.ErrorContains(t, err, testCase.error)
		})
	}
}

func TestNew_OptionsRetryDefaultsIndependently(t *testing.T) {
	t.Parallel()

	const (
		defaultMaxAttempts = 3
		defaultBaseDelay   = 500 * time.Millisecond
		defaultMaxDelay    = 30 * time.Second
	)

	tests := []struct {
		name                string
		options             cord.Options
		maxAttempts         int
		baseDelay, maxDelay time.Duration
	}{
		{
			name:        "all zero",
			maxAttempts: defaultMaxAttempts, baseDelay: defaultBaseDelay, maxDelay: defaultMaxDelay,
		},
		{
			name: "max attempts only", options: cord.Options{MaxAttempts: 5},
			maxAttempts: 5, baseDelay: defaultBaseDelay, maxDelay: defaultMaxDelay,
		},
		{
			name: "base delay only", options: cord.Options{RetryBaseDelay: time.Second},
			maxAttempts: defaultMaxAttempts, baseDelay: time.Second, maxDelay: defaultMaxDelay,
		},
		{
			name: "maximum delay only", options: cord.Options{RetryMaxDelay: time.Minute},
			maxAttempts: defaultMaxAttempts, baseDelay: defaultBaseDelay, maxDelay: time.Minute,
		},
		{
			name: "max attempts and base delay", options: cord.Options{
				MaxAttempts: 5, RetryBaseDelay: time.Second,
			},
			maxAttempts: 5, baseDelay: time.Second, maxDelay: defaultMaxDelay,
		},
		{
			name: "max attempts and maximum delay", options: cord.Options{
				MaxAttempts: 5, RetryMaxDelay: time.Minute,
			},
			maxAttempts: 5, baseDelay: defaultBaseDelay, maxDelay: time.Minute,
		},
		{
			name: "base and maximum delays", options: cord.Options{
				RetryBaseDelay: time.Second, RetryMaxDelay: time.Minute,
			},
			maxAttempts: defaultMaxAttempts, baseDelay: time.Second, maxDelay: time.Minute,
		},
		{
			name: "fully specified", options: cord.Options{
				MaxAttempts: 5, RetryBaseDelay: time.Second, RetryMaxDelay: time.Minute,
			},
			maxAttempts: 5, baseDelay: time.Second, maxDelay: time.Minute,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assertRetryOptions(t, testCase.options, testCase.maxAttempts, testCase.baseDelay, testCase.maxDelay)
		})
	}
}

func assertRetryOptions(
	t *testing.T,
	options cord.Options,
	wantMaxAttempts int,
	wantBaseDelay, wantMaxDelay time.Duration,
) {
	t.Helper()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	_, err = runtime.From("retry-defaults", increment).Run(t.Context(), 1)
	require.NoError(t, err)

	var (
		maxAttempts             int
		baseDelayNS, maxDelayNS int64
	)
	require.NoError(t, database.QueryRowContext(t.Context(), `
		SELECT max_attempts, retry_base_delay_ns, retry_max_delay_ns
		FROM cord_runs
	`).Scan(&maxAttempts, &baseDelayNS, &maxDelayNS))

	assert.Equal(t, wantMaxAttempts, maxAttempts)
	assert.Equal(t, wantBaseDelay, time.Duration(baseDelayNS))
	assert.Equal(t, wantMaxDelay, time.Duration(maxDelayNS))
}

func TestNew_OptionsValidatesBeforeMigration(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database, cord.Options{Concurrency: -1})

	assert.Nil(t, runtime)
	require.ErrorContains(t, err, "concurrency")
	assert.False(t, sqliteTableExists(t, database, "cord_schema_migrations"))
}

func TestNew_OptionsLeasePrecisionBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options cord.Options
	}{
		{name: "derived heartbeat", options: cord.Options{LeaseTTL: 30 * time.Millisecond}},
		{name: "clamped derived heartbeat", options: cord.Options{LeaseTTL: 2*time.Millisecond + time.Nanosecond}},
		{name: "one millisecond heartbeat", options: cord.Options{
			LeaseTTL: 2*time.Millisecond + time.Nanosecond, HeartbeatInterval: time.Millisecond,
		}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runtime, err := cord.New(t.Context(), openSQLite(t), testCase.options)
			require.NoError(t, err)
			require.NoError(t, runtime.Close())
		})
	}
}

func TestNew_CanceledMigrationContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	runtime, err := cord.New(ctx, openSQLite(t))

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, context.Canceled)
}

func TestNew_RejectsMultipleOptions(t *testing.T) {
	t.Parallel()

	runtime, err := cord.New(t.Context(), openSQLite(t), cord.Options{}, cord.Options{})

	assert.Nil(t, runtime)
	require.ErrorContains(t, err, "at most one options")
}

func TestNew_RejectsNilDatabase(t *testing.T) {
	t.Parallel()

	runtime, err := cord.New(t.Context(), nil)

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrMigrationFailed)
}

func TestNew_MigratesAndLeavesDatabaseOpen(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NotNil(t, runtime)

	for _, table := range []string{"cord_schema_migrations", "cord_runs", "cord_nodes", "cord_edges"} {
		assert.True(t, sqliteTableExists(t, database, table), table)
	}

	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
	require.NoError(t, database.PingContext(t.Context()))
}

func TestNew_IsRepeatable(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)

	first, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	second, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, second.Close())

	var applied int

	err = database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_schema_migrations WHERE version_id = 1 AND is_applied = 1",
	).Scan(&applied)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
}

func TestNew_ConcurrentCalls(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "concurrent.db") +
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"

	const constructors = 8

	var waitGroup sync.WaitGroup

	results := make(chan error, constructors)

	for range constructors {
		waitGroup.Go(func() {
			database, err := sql.Open("sqlite", dsn)
			if err != nil {
				results <- err

				return
			}

			runtime, err := cord.New(t.Context(), database)
			if err == nil {
				err = runtime.Close()
			}

			if closeErr := database.Close(); err == nil {
				err = closeErr
			}

			results <- err
		})
	}

	waitGroup.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}
}

func TestNew_MigratesOldSchema(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, runtime.Close())

	for _, statement := range []string{
		"DROP TABLE cord_edges",
		"DROP TABLE cord_nodes",
		"DROP TABLE cord_runs",
		"DELETE FROM cord_schema_migrations",
		"INSERT INTO cord_schema_migrations (version_id, is_applied) VALUES (0, 1)",
	} {
		_, err = database.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}

	runtime, err = cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, runtime.Close())

	for _, table := range []string{"cord_runs", "cord_nodes", "cord_edges"} {
		assert.True(t, sqliteTableExists(t, database, table), table)
	}

	var applied int

	err = database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_schema_migrations WHERE version_id = 1 AND is_applied = 1",
	).Scan(&applied)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
}

func TestNew_FailedMigrationRollsBack(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	_, err := database.ExecContext(t.Context(), "CREATE TABLE cord_nodes (id TEXT PRIMARY KEY)")
	require.NoError(t, err)

	runtime, err := cord.New(t.Context(), database)

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrMigrationFailed)
	assert.False(t, sqliteTableExists(t, database, "cord_runs"))
	assert.False(t, sqliteTableExists(t, database, "cord_edges"))
	assert.True(t, sqliteTableExists(t, database, "cord_nodes"))

	var applied int

	err = database.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM cord_schema_migrations WHERE version_id = 1 AND is_applied = 1",
	).Scan(&applied)
	require.NoError(t, err)
	assert.Zero(t, applied)
}

func TestNew_RejectsNewerSchema(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database)
	require.NoError(t, err)
	require.NoError(t, runtime.Close())

	_, err = database.ExecContext(
		t.Context(),
		"INSERT INTO cord_schema_migrations (version_id, is_applied) VALUES (6, 1)",
	)
	require.NoError(t, err)

	runtime, err = cord.New(t.Context(), database)

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrSchemaNewer)
	require.ErrorContains(t, err, "current=")
	require.ErrorContains(t, err, "required=")
}

func TestNew_ReportsDatabaseFailure(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	require.NoError(t, database.Close())

	runtime, err := cord.New(t.Context(), database)

	assert.Nil(t, runtime)
	require.ErrorIs(t, err, cord.ErrMigrationFailed)
}

func TestWorkflow_RunPersistsReachablePlan(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime := newRuntimeForDB(t, database)

	root := runtime.From("test-workflow", increment)
	selected := root.Then(double)
	_ = root.Then(increment)

	result, err := selected.Run(t.Context(), 3)
	require.NoError(t, err)
	assert.Equal(t, 8, result)

	var (
		runID           string
		workflowName    string
		definitionHash  string
		status          string
		input           []byte
		rootFunctionKey string
		nodeCount       int
		edgeCount       int
	)

	err = database.QueryRowContext(
		t.Context(),
		"SELECT id, workflow_name, definition_hash, status, input_payload FROM cord_runs",
	).Scan(&runID, &workflowName, &definitionHash, &status, &input)
	require.NoError(t, err)
	require.NoError(t, database.QueryRowContext(
		t.Context(),
		"SELECT function_key FROM cord_nodes WHERE remaining_deps = 0",
	).Scan(&rootFunctionKey))
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_nodes").Scan(&nodeCount))
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_edges").Scan(&edgeCount))

	identifier, parseErr := uuid.FromString(runID)
	require.NoError(t, parseErr)
	assert.Equal(t, uuid.V7, identifier.Version())
	assert.Equal(t, "test-workflow", workflowName)
	assert.NotEqual(t, rootFunctionKey, workflowName)
	assert.Len(t, definitionHash, 64)
	assert.Equal(t, "completed", status)
	assert.JSONEq(t, "3", string(input))
	assert.Equal(t, 2, nodeCount)
	assert.Equal(t, 1, edgeCount)
}

func TestWorkflow_RunRejectsClosureBeforeInsertion(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime := newRuntimeForDB(t, database)

	called := false
	workflow := runtime.From("test-workflow", func(_ context.Context, value int) (int, error) {
		called = true

		return value, nil
	})

	_, err := workflow.Run(t.Context(), 1)
	require.ErrorContains(t, err, "not a named package-level function")
	assert.False(t, called)

	var count int
	require.NoError(t, database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cord_runs").Scan(&count))
	assert.Zero(t, count)
}

func sqliteTableExists(t *testing.T, database *sql.DB, table string) bool {
	t.Helper()

	var name string

	err := database.QueryRowContext(
		t.Context(),
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&name)

	return err == nil && name == table
}
