// Command linear demonstrates composing workflow steps in a linear chain.
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/exampledb"
)

func increment(_ context.Context, value int) (int, error) { return value + 1, nil }
func double(_ context.Context, value int) (int, error)    { return value * 2, nil }
func formatResult(_ context.Context, value int) (string, error) {
	return fmt.Sprintf("result: %d", value), nil
}

func run(ctx context.Context, input int) (_ string, err error) {
	database := exampledb.DB()

	runtime, err := cord.New(database)
	if err != nil {
		return "", fmt.Errorf("create runtime: %w", err)
	}

	defer func() {
		err = errors.Join(err, runtime.Close(), database.Close())
	}()

	flow := runtime.From(increment).Then(double).Then(formatResult)

	return flow.Run(ctx, input)
}

func main() {
	result, err := run(context.Background(), 4)
	fmt.Println(result, err)
}
