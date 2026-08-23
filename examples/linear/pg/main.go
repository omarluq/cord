// Command pg runs the linear workflow example with pgx's database/sql driver.
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
	database, err := exampledb.OpenPostgres(ctx)
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
