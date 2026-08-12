// Command linear demonstrates composing workflow steps in a linear chain.
package main

import (
	"context"
	"fmt"

	"github.com/omarluq/cord"
)

func run(ctx context.Context, input int) (string, error) {
	runtime := cord.New()
	flow := runtime.From("calculate", func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	}).Then(func(_ context.Context, value int) (int, error) {
		return value * 2, nil
	}).Then(func(_ context.Context, value int) (string, error) {
		return fmt.Sprintf("result: %d", value), nil
	})

	return flow.Run(ctx, input)
}

func main() {
	result, err := run(context.Background(), 4)
	fmt.Println(result, err)
}
