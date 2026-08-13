// Command join demonstrates joining two workflow branches.
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/exampledb"
)

func loadReport(_ context.Context, value int) (int, error) { return value, nil }
func double(_ context.Context, value int) (int, error)     { return value * 2, nil }
func addThree(_ context.Context, value int) (int, error)   { return value + 3, nil }
func sum(_ context.Context, left, right int) (int, error)  { return left + right, nil }

func run(ctx context.Context, input int) (_ int, err error) {
	database := exampledb.DB()

	runtime, err := cord.New(database)
	if err != nil {
		return 0, fmt.Errorf("create runtime: %w", err)
	}

	defer func() {
		err = errors.Join(err, runtime.Close(), database.Close())
	}()

	root := runtime.From(loadReport)
	flow := cord.Join(root.Then(double), root.Then(addThree)).Then(sum)

	return flow.Run(ctx, input)
}

func main() {
	result, err := run(context.Background(), 4)
	fmt.Println(result, err)
}
