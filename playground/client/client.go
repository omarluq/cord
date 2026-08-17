// Package client provides the browser bridge used by programs compiled by the
// Cord playground.
package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/omarluq/cord"

	// Register the browser-compatible SQLite driver and its in-memory VFS.
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/vfs/memdb"
)

const (
	databaseDSN   = "file:/cord-user-workflow.db?vfs=memdb&_pragma=foreign_keys(1)"
	eventStateKey = "state"
)

// Node describes one node displayed in the playground graph.
type Node struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// Edge describes a directed dependency displayed in the playground graph.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Session owns one browser-local Cord runtime and SQLite database.
type Session struct {
	Cord     *cord.Cord
	database *sql.DB
}

// NewSession starts a browser-local Cord runtime backed by in-memory SQLite.
func NewSession(ctx context.Context) (*Session, error) {
	database, err := sql.Open("sqlite3", databaseDSN)
	if err != nil {
		return nil, fmt.Errorf("open playground database: %w", err)
	}
	database.SetMaxOpenConns(1)

	runtime, err := cord.New(ctx, database, cord.Options{
		Concurrency:       4,
		PollInterval:      25 * time.Millisecond,
		LeaseTTL:          5 * time.Second,
		HeartbeatInterval: time.Second,
		MaxAttempts:       5,
		RetryBaseDelay:    150 * time.Millisecond,
		RetryMaxDelay:     600 * time.Millisecond,
		OnSchedulerError:  func(error) {},
	})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("start Cord: %w", err), database.Close())
	}

	return &Session{Cord: runtime, database: database}, nil
}

// Close stops Cord before closing its caller-owned database.
func (session *Session) Close() error {
	if session == nil {
		return nil
	}

	return errors.Join(session.Cord.Close(), session.database.Close())
}

// Graph publishes the workflow graph before execution begins.
func Graph(nodes []Node, edges []Edge) {
	emit("graph", map[string]any{"nodes": nodes, "edges": edges})
}

// Step records the lifecycle of a user workflow step.
func Step[T any](name string, run func() (T, error)) (T, error) {
	emit("node", map[string]any{"id": name, eventStateKey: "running"})
	value, err := run()
	if err != nil {
		emit("node", map[string]any{"id": name, eventStateKey: "failed", "message": err.Error()})
		return value, err
	}

	emit("node", map[string]any{"id": name, eventStateKey: "completed"})
	return value, nil
}

// Result publishes a successful workflow result.
func Result(value any) {
	emit("result", map[string]any{"value": fmt.Sprint(value)})
}

// EmitError publishes an execution error.
func EmitError(err error) {
	if err != nil {
		emit("error", map[string]any{"message": err.Error()})
	}
}
