// Command pg runs the joined workflow example with pgx's database/sql driver.
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
	if err := examplecmd.Run(ctx, exampledb.OpenPostgres, joinexample.Run, exampleInput); err != nil {
		return fmt.Errorf("run example: %w", err)
	}

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
