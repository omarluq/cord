// Command sqlite runs the joined workflow example with modernc SQLite.
package main

import (
	"context"
	"log"

	joinexample "github.com/omarluq/cord/examples/join"
	"github.com/omarluq/cord/internal/examplecmd"
	"github.com/omarluq/cord/internal/exampledb"
)

func run(ctx context.Context) error {
	return examplecmd.Run(ctx, exampledb.OpenSQLite, joinexample.Run, 4)
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
