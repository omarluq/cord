// Command sqlite runs the linear workflow example with modernc SQLite.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/omarluq/cord/examples/linear"
	"github.com/omarluq/cord/internal/examplecmd"
	"github.com/omarluq/cord/internal/exampledb"
)

const exampleInput = 4

func run(ctx context.Context) error {
	if err := examplecmd.Run(ctx, exampledb.OpenSQLite, linear.Run, exampleInput); err != nil {
		return fmt.Errorf("run example: %w", err)
	}

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
