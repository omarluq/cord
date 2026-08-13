// Command join demonstrates joining two workflow branches.
package main

import (
	"context"
	"fmt"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/exampledb"
)

func loadReport(_ context.Context, value int) (int, error) { return value, nil }
func double(_ context.Context, value int) (int, error)     { return value * 2, nil }
func addThree(_ context.Context, value int) (int, error)   { return value + 3, nil }
func sum(_ context.Context, left, right int) (int, error)  { return left + right, nil }

func run(ctx context.Context, input int) (int, error) {
	runtime, err := cord.New(exampledb.DB())
	if err != nil {
		return 0, fmt.Errorf("create runtime: %w", err)
	}

	root := runtime.From(loadReport)
	flow := cord.Join(root.Then(double), root.Then(addThree)).Then(sum)

	return flow.Run(ctx, input)
}

func main() {
	result, err := run(context.Background(), 4)
	fmt.Println(result, err)
}
