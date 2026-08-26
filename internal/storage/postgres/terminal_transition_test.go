package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuccessfulTerminalCompletionLeavesUnfinishedNodesNonterminal(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	require.NoError(t, postgres.Migrate(t.Context(), database))
	store, err := postgres.New(database)
	require.NoError(t, err)

	const runID storage.RunID = "successful-terminal-with-unfinished-node"

	plan := terminalRacePlan(runID)
	require.NoError(t, store.CreateRun(t.Context(), &plan))

	_, err = database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET status = $1, remaining_deps = 0
		WHERE run_id = $2 AND node_id = $3`, storage.NodeReady, runID, "terminal")
	require.NoError(t, err)

	terminal := claimPostgresNode(t, store, "terminal-worker", "terminal-key", "terminal-signature")
	sibling := claimPostgresNode(t, store, "sibling-worker", "sibling-key", "sibling-signature")

	accepted, err := store.CompleteNode(
		t.Context(), terminal.RunID, terminal.NodeID, terminal.Lease, []byte(`"done"`),
	)
	require.NoError(t, err)
	require.True(t, accepted)

	var (
		status storage.NodeStatus
		reason sql.NullString
	)

	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT status, terminal_reason
		FROM cord_nodes WHERE run_id = $1 AND node_id = $2`, runID, sibling.NodeID).Scan(&status, &reason))
	assert.Equal(t, storage.NodeRunning, status)
	assert.False(t, reason.Valid)
}

func TestTerminalTransitionsSerializeOnRun(t *testing.T) {
	t.Parallel()

	database := openPostgres(t, startPostgres(t))
	require.NoError(t, postgres.Migrate(t.Context(), database))
	store, err := postgres.New(database)
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
			storage.ReasonFailureAttemptsExhausted,
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
	store *postgres.Store,
	runID storage.RunID,
	terminalTransition terminalTransition,
	completionAllowed storage.RunStatus,
) {
	t.Helper()

	plan := terminalRacePlan(runID)
	err := store.CreateRun(t.Context(), &plan)
	require.NoError(t, err)

	// This low-level race fixture intentionally makes an ancestor and its
	// terminal child claimable together after validating and persisting a legal
	// plan. Public plan validation correctly rejects this topology as an input.
	_, err = database.ExecContext(t.Context(), `UPDATE cord_nodes
		SET status = $1, remaining_deps = 0
		WHERE run_id = $2 AND node_id = $3`, storage.NodeReady, runID, "terminal")
	require.NoError(t, err)

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
			storage.ReasonFailureAttemptsExhausted,
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
