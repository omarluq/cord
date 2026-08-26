// Package postgres implements Cord's PostgreSQL persistence adapter.
package postgres

import (
	"database/sql"
	"errors"

	"github.com/omarluq/cord/internal/storage"
)

// Store persists workflow state in a caller-owned PostgreSQL database.
type Store struct{ pool *sql.DB }

var _ storage.Backend = (*Store)(nil)

// New creates a PostgreSQL store for a caller-owned SQL database.
func New(database *sql.DB) (*Store, error) {
	if database != nil {
		return &Store{pool: database}, nil
	}

	return nil, errors.New("create postgres store: database is nil")
}
