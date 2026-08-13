package cord_test

import (
	"context"
	"fmt"

	"github.com/omarluq/cord"
)

func ExampleCord_From() {
	flow := cord.New().From("double", func(_ context.Context, value int) (int, error) {
		return value * 2, nil
	}).Then(func(_ context.Context, value int) (string, error) {
		return fmt.Sprintf("result: %d", value), nil
	})

	result, err := flow.Run(context.Background(), 21)
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(result)
	// Output: result: 42
}

func ExampleJoin() {
	runtime := cord.New()
	root := runtime.From("order", func(_ context.Context, orderID int) (int, error) {
		return orderID, nil
	})
	items := root.Then(func(_ context.Context, _ int) (int, error) {
		return 3, nil
	})
	shipping := root.Then(func(_ context.Context, _ int) (int, error) {
		return 5, nil
	})
	flow := cord.Join(items, shipping).Then(func(_ context.Context, itemCost, shippingCost int) (int, error) {
		return itemCost + shippingCost, nil
	})

	total, err := flow.Run(context.Background(), 1001)
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(total)
	// Output: 8
}
