// Command sqlite runs the joined workflow example with modernc SQLite.
package main

import (
	"context"
	"fmt"
	"log"

	joinexample "github.com/omarluq/cord/examples/join"
	"github.com/omarluq/cord/internal/examplecmd"
	"github.com/omarluq/cord/internal/exampledb"
)

const exampleInput = 4

func run(ctx context.Context) error {
	if err := examplecmd.Run(ctx, exampledb.OpenSQLite, joinexample.Run, exampleInput); err != nil {
		return fmt.Errorf("run example: %w", err)
	}

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
