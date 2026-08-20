// Package conformance verifies storage backend behavior.
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

// Driver configures a database/sql driver for the conformance suite.
type Driver struct {
	// DataSource builds a driver data source from a temporary path and busy timeout.
	DataSource func(string, time.Duration) string
	// Open optionally opens the database and takes precedence over Name and DataSource.
	Open func(testing.TB) *sql.DB
	// Name identifies the registered database/sql driver.
	Name string
	// SkipWriteContention skips the local-file write-contention test.
	SkipWriteContention bool
}

// RepeatedPragmaDataSource builds a data source for drivers that accept repeated _pragma parameters.
func RepeatedPragmaDataSource(path string, timeout time.Duration) string {
	query := url.Values{}
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", timeout.Milliseconds()))
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "journal_mode(WAL)")

	return "file:" + path + "?" + query.Encode()
}

// UnderscoreDataSource builds a data source for drivers that accept underscore-prefixed options.
func UnderscoreDataSource(path string, timeout time.Duration) string {
	query := url.Values{}
	query.Set("_busy_timeout", strconv.FormatInt(timeout.Milliseconds(), 10))
	query.Set("_foreign_keys", "on")
	query.Set("_journal_mode", "WAL")

	return "file:" + path + "?" + query.Encode()
}

// Run executes Cord's behavioral storage conformance suite.
func Run(t *testing.T, driver Driver) {
	t.Helper()

	open := func(tb testing.TB, path string, timeout time.Duration) *sql.DB {
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

	t.Run("workflow", func(t *testing.T) { runWorkflow(t, open) })

	if !driver.SkipWriteContention {
		t.Run("write contention", func(t *testing.T) { runContention(t, open) })
	}
}

// Open opens a backend-specific test database.
type Open func(testing.TB, string, time.Duration) *sql.DB

func runWorkflow(t *testing.T, open Open) {
	t.Helper()

	const (
		busyTimeout = 5 * time.Second
		input       = 2
		want        = 4
	)

	database := open(t, filepath.Join(t.TempDir(), "workflow.db"), busyTimeout)

	runtime, err := cord.New(t.Context(), database, cord.Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		closeErr := runtime.Close()
		if closeErr != nil {
			t.Errorf("close runtime: %v", closeErr)
		}
	})

	workflow := runtime.From("sqlite-driver-conformance", timesTwo)

	result, err := workflow.Run(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}

	if result != want {
		t.Fatalf("result = %d, want %d", result, want)
	}
}

func runContention(t *testing.T, open Open) {
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

	verifyRetryWhileLocked(t, store, transaction)
}

func verifyRetryWhileLocked(t *testing.T, store *sqlite.Store, transaction *sql.Tx) {
	t.Helper()

	const lockObservationDelay = 20 * time.Millisecond

	now := time.Now().UTC()
	runID := storage.RunID(fmt.Sprintf("sqlite-driver-contention-%d", now.UnixNano()))

	result := make(chan error, 1)
	go func() {
		result <- store.CreateRun(t.Context(), contentionPlan(runID, now))
	}()

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

func timesTwo(_ context.Context, input int) (int, error) {
	const multiplier = 2

	return input * multiplier, nil
}

func contentionPlan(runID storage.RunID, now time.Time) *storage.RunPlan {
	return &storage.RunPlan{
		Edges: nil,
		Run: storage.Run{
			CompletedAt: nil, Output: nil, Error: nil,
			ID: runID, WorkflowName: "sqlite-driver-contention", DefinitionHash: "definition",
			TerminalNodeID: "node", Status: storage.RunRunning, Input: []byte(`1`), CreatedAt: now, UpdatedAt: now,
			MaxAttempts: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond, RetryPolicyVersion: 1,
		},
		Nodes: []storage.Node{{
			CompletedAt: nil, StartedAt: nil, Lease: storage.Lease{}, Error: nil, Output: nil,
			RunID: runID, ID: "node", FunctionKey: "example.com/Step", SignatureHash: "signature",
			Status: storage.NodeReady, AvailableAt: now, RemainingDeps: 0, Attempt: 0,
		}},
	}
}
