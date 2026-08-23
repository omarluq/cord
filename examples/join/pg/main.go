// Command pg runs the joined workflow example with pgx's database/sql driver.
package main

import (
	"context"
	"log"

	joinexample "github.com/omarluq/cord/examples/join"
	"github.com/omarluq/cord/internal/examplecmd"
	"github.com/omarluq/cord/internal/exampledb"
)

func run(ctx context.Context) error {
	return examplecmd.Run(ctx, exampledb.OpenPostgres, joinexample.Run, 4)
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
