// Package conformance verifies storage backend behavior.
package conformance

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlite"
)

// Open opens a backend-specific test database.
type Open func(testing.TB, string, time.Duration) *sql.DB

// Run executes Cord's behavioral storage conformance suite.
func Run(t *testing.T, open Open) {
	t.Helper()
	t.Run("workflow", func(t *testing.T) { runWorkflow(t, open) })
	t.Run("write contention", func(t *testing.T) { runContention(t, open) })
}

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

	const releaseDelay = 20 * time.Millisecond

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

	released := releaseTransaction(t.Context(), transaction, releaseDelay)

	if err := store.CreateRun(t.Context(), contentionPlan(time.Now().UTC())); err != nil {
		t.Fatal(err)
	}

	if err := <-released; err != nil {
		t.Fatal(err)
	}
}

func releaseTransaction(ctx context.Context, transaction *sql.Tx, delay time.Duration) <-chan error {
	released := make(chan error, 1)

	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			released <- ctx.Err()
		case <-timer.C:
			released <- transaction.Rollback()
		}
	}()

	return released
}

func timesTwo(_ context.Context, input int) (int, error) {
	const multiplier = 2

	return input * multiplier, nil
}

func contentionPlan(now time.Time) *storage.RunPlan {
	const runID = storage.RunID("sqlite-driver-contention")

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
