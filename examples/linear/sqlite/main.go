// Command sqlite runs the linear workflow example with modernc SQLite.
package main

import (
	"context"
	"log"

	"github.com/omarluq/cord/examples/linear"
	"github.com/omarluq/cord/internal/examplecmd"
	"github.com/omarluq/cord/internal/exampledb"
)

func run(ctx context.Context) error {
	return examplecmd.Run(ctx, exampledb.OpenSQLite, linear.Run, 4)
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
