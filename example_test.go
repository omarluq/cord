package cord_test

import (
	"context"
	"fmt"

	"github.com/omarluq/cord"
	"github.com/omarluq/cord/internal/exampledb"
)

func exampleDouble(_ context.Context, value int) (int, error) { return value * 2, nil }
func exampleFormat(_ context.Context, value int) (string, error) {
	return fmt.Sprintf("result: %d", value), nil
}
func exampleOrder(_ context.Context, orderID int) (int, error)         { return orderID, nil }
func exampleItems(_ context.Context, _ int) (int, error)               { return 3, nil }
func exampleShipping(_ context.Context, _ int) (int, error)            { return 5, nil }
func exampleTotal(_ context.Context, items, shipping int) (int, error) { return items + shipping, nil }

func ExampleCord_From() {
	runtime, err := cord.New(exampledb.DB())
	if err != nil {
		fmt.Println(err)

		return
	}

	result, err := runtime.From(exampleDouble).Then(exampleFormat).Run(context.Background(), 21)
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(result)
	// Output: result: 42
}

func ExampleJoin() {
	runtime, err := cord.New(exampledb.DB())
	if err != nil {
		fmt.Println(err)

		return
	}

	root := runtime.From(exampleOrder)
	flow := cord.Join(root.Then(exampleItems), root.Then(exampleShipping)).Then(exampleTotal)

	total, err := flow.Run(context.Background(), 1001)
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(total)
	// Output: 8
}
