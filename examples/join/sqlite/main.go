// Command sqlite runs the joined workflow example with modernc SQLite.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	joinexample "github.com/omarluq/cord/examples/join"
	"github.com/omarluq/cord/internal/exampledb"
)

func run(ctx context.Context) (err error) {
	database, err := exampledb.OpenSQLite()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, database.Close()) }()

	result, err := joinexample.Run(ctx, database, 4)
	if err != nil {
		return fmt.Errorf("run joined workflow: %w", err)
	}

	fmt.Println(result)

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
