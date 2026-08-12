// Command join demonstrates joining two workflow branches.
package main

import (
	"context"
	"fmt"

	"github.com/omarluq/cord"
)

func run(ctx context.Context, input int) (int, error) {
	runtime := cord.New()
	root := runtime.From("report", func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	left := root.Then(func(_ context.Context, value int) (int, error) {
		return value * 2, nil
	})
	right := root.Then(func(_ context.Context, value int) (int, error) {
		return value + 3, nil
	})
	flow := cord.Join(left, right).Then(func(_ context.Context, leftValue, rightValue int) (int, error) {
		return leftValue + rightValue, nil
	})

	return flow.Run(ctx, input)
}

func main() {
	result, err := run(context.Background(), 4)
	fmt.Println(result, err)
}
