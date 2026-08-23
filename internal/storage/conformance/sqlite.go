package conformance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
)

// SQLiteDriver configures a database/sql SQLite driver for conformance tests.
type SQLiteDriver struct {
	// DataSource builds a driver data source from a temporary path and busy timeout.
	DataSource func(string, time.Duration) string
	// Open optionally opens the database and takes precedence over Name and DataSource.
	Open func(testing.TB) *sql.DB
	// Name identifies the registered database/sql driver.
	Name string
	// SkipWriteContention skips the local-file write-contention test.
	SkipWriteContention bool
}

// RepeatedPragmaDataSource builds a data source for SQLite drivers that accept repeated _pragma parameters.
func RepeatedPragmaDataSource(path string, timeout time.Duration) string {
	query := url.Values{}
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", timeout.Milliseconds()))
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "journal_mode(WAL)")

	return "file:" + path + "?" + query.Encode()
}

// UnderscoreDataSource builds a data source for SQLite drivers that accept underscore-prefixed options.
func UnderscoreDataSource(path string, timeout time.Duration) string {
	query := url.Values{}
	query.Set("_busy_timeout", strconv.FormatInt(timeout.Milliseconds(), 10))
	query.Set("_foreign_keys", "on")
	query.Set("_journal_mode", "WAL")

	return "file:" + path + "?" + query.Encode()
}

// RunSQLite executes the common backend suite and SQLite-specific conformance cases.
func RunSQLite(t *testing.T, driver SQLiteDriver) {
	t.Helper()

	openAt := sqliteOpener(driver)
	harness := Harness{
		Open: func(tb testing.TB, name string) *sql.DB {
			tb.Helper()

			return openAt(tb, filepath.Join(tb.TempDir(), name+".db"), storeBusyTimeout)
		},
		Migrate: sqlite.Migrate,
		NewBackend: func(database *sql.DB) (storage.Backend, error) {
			return sqlite.New(database)
		},
		ExpireLease: func(ctx context.Context, database *sql.DB, runID storage.RunID, nodeID storage.NodeID) error {
			_, err := database.ExecContext(ctx, `UPDATE cord_nodes
				SET lease_expires_at = '2000-01-01T00:00:00.000Z' WHERE run_id = ? AND node_id = ?`, runID, nodeID)
			if err != nil {
				return fmt.Errorf("expire SQLite lease: %w", err)
			}

			return nil
		},
		DeleteRun: deleteSQLiteRun,
	}

	Run(t, harness)
	t.Run("workflow", func(t *testing.T) { runSQLiteWorkflow(t, harness) })

	if !driver.SkipWriteContention {
		t.Run("write contention", func(t *testing.T) { runSQLiteContention(t, openAt) })
	}
}

const storeBusyTimeout = 5 * time.Second

type sqliteOpen func(testing.TB, string, time.Duration) *sql.DB

func sqliteOpener(driver SQLiteDriver) sqliteOpen {
	return func(tb testing.TB, path string, timeout time.Duration) *sql.DB {
		tb.Helper()

		var database *sql.DB
		if driver.Open != nil {
			database = driver.Open(tb)
		} else {
			var err error

			database, err = sql.Open(driver.Name, driver.DataSource(path, timeout))
			if err != nil {
				tb.Fatal(err)
			}
		}

		tb.Cleanup(func() {
			if err := database.Close(); err != nil {
				tb.Errorf("close database: %v", err)
			}
		})

		return database
	}
}

func deleteSQLiteRun(ctx context.Context, database *sql.DB, runID storage.RunID) error {
	if _, err := database.ExecContext(ctx, "DELETE FROM cord_runs WHERE id = ?", runID); err != nil {
		return fmt.Errorf("delete SQLite run: %w", err)
	}

	queries := map[string]string{
		"cord_nodes": "SELECT COUNT(*) FROM cord_nodes WHERE run_id = ?",
		"cord_edges": "SELECT COUNT(*) FROM cord_edges WHERE run_id = ?",
	}
	for table, query := range queries {
		var count int

		row := database.QueryRowContext(ctx, query, runID)
		if err := row.Scan(&count); err != nil {
			return fmt.Errorf("count SQLite %s rows: %w", table, err)
		}

		if count != 0 {
			return fmt.Errorf("%s rows after run deletion = %d, want 0", table, count)
		}
	}

	return nil
}

func runSQLiteWorkflow(t *testing.T, harness Harness) {
	t.Helper()

	database := harness.Open(t, "workflow")

	runtime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if closeErr := runtime.Close(); closeErr != nil {
			t.Errorf("close runtime: %v", closeErr)
		}
	})

	const input, want = 2, 4

	workflow := runtime.From("sqlite-driver-conformance", timesTwo)

	result, err := workflow.Run(t.Context(), input)
	if err != nil || result != want {
		t.Fatalf("result = %d, want %d, err=%v", result, want, err)
	}
}

func runSQLiteContention(t *testing.T, open sqliteOpen) {
	t.Helper()

	_, store, transaction := lockedSQLiteStore(t, open)

	const lockObservationDelay = 20 * time.Millisecond

	now := time.Now().UTC()
	runID := storage.RunID(fmt.Sprintf("sqlite-driver-contention-%d", now.UnixNano()))

	result := make(chan error, 1)
	go func() { result <- store.CreateRun(t.Context(), contentionPlan(runID, now)) }()

	select {
	case err := <-result:
		t.Fatalf("create run returned while the write lock was held: %v", err)
	case <-time.After(lockObservationDelay):
	}

	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func lockedSQLiteStore(t *testing.T, open sqliteOpen) (*sql.DB, *sqlite.Store, *sql.Tx) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "contention.db")
	first := open(t, path, 0)

	second := open(t, path, 0)
	if err := sqlite.Migrate(t.Context(), first); err != nil {
		t.Fatal(err)
	}

	store, err := sqlite.New(second)
	if err != nil {
		t.Fatal(err)
	}

	transaction, err := first.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("rollback transaction: %v", err)
		}
	})

	if _, err := transaction.ExecContext(t.Context(), "UPDATE cord_runs SET status = status"); err != nil {
		t.Fatal(err)
	}

	return first, store, transaction
}

func timesTwo(_ context.Context, input int) (int, error) {
	const multiplier = 2

	return input * multiplier, nil
}

func contentionPlan(runID storage.RunID, now time.Time) *storage.RunPlan {
	plan := singleNodePlan(runID, "sqlite-driver-contention")
	plan.Run.CreatedAt, plan.Run.UpdatedAt = now, now
	plan.Run.MaxAttempts = 1
	plan.Nodes[0].AvailableAt = now

	return &plan
}
