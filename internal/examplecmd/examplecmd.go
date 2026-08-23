// Package examplecmd provides shared execution plumbing for executable examples.
package examplecmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
)

// DatabaseOpener opens a database used by an executable example.
type DatabaseOpener func(context.Context) (*sql.DB, error)

// Run opens a database, executes a workflow, prints its result, and closes the database.
func Run[T any](
	ctx context.Context,
	open DatabaseOpener,
	workflow func(context.Context, *sql.DB, int) (T, error),
	input int,
) (err error) {
	database, err := open(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, database.Close()) }()

	result, err := workflow(ctx, database, input)
	if err != nil {
		return fmt.Errorf("run workflow: %w", err)
	}

	if _, err := fmt.Fprintln(os.Stdout, result); err != nil {
		return fmt.Errorf("write result: %w", err)
	}

	return nil
}
