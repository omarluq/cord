// Package exampledb provides SQLite databases for examples.
package exampledb

import (
	"database/sql"

	// Register the SQLite driver used by examples.
	_ "modernc.org/sqlite"
)

// DB opens an in-memory SQLite database suitable for an example runtime.
// Examples can leave it open because their process owns the database lifetime.
func DB() *sql.DB {
	database, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(1)")
	if err != nil {
		panic(err)
	}

	database.SetMaxOpenConns(1)

	return database
}
