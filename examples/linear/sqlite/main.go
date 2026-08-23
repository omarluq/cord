// Command sqlite runs the linear workflow example with modernc SQLite.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/omarluq/cord/examples/linear"
	"github.com/omarluq/cord/internal/exampledb"
)

func run(ctx context.Context) (err error) {
	database, err := exampledb.OpenSQLite()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, database.Close()) }()

	result, err := linear.Run(ctx, database, 4)
	if err != nil {
		return fmt.Errorf("run linear workflow: %w", err)
	}

	fmt.Println(result)

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
