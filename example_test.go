package cord_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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

func examplePermanent(_ context.Context, value int) (int, error) {
	return value, fmt.Errorf("invalid input: %w", cord.Permanent(errors.New("terminal")))
}

func closeExample(runtime *cord.Cord, database *sql.DB) error {
	return errors.Join(runtime.Close(), database.Close())
}

func ExampleCord_From() {
	database := exampledb.DB()

	runtime, err := cord.New(context.Background(), database)
	if err != nil {
		fmt.Println(err)

		return
	}

	result, runErr := runtime.From("double-and-format", exampleDouble).Then(exampleFormat).Run(context.Background(), 21)
	if err := errors.Join(runErr, closeExample(runtime, database)); err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(result)
	// Output: result: 42
}

func ExampleWorkflow_Submit() {
	database := exampledb.DB()

	runtime, err := cord.New(context.Background(), database)
	if err != nil {
		fmt.Println(errors.Join(err, database.Close()))

		return
	}
	defer func() {
		if closeErr := closeExample(runtime, database); closeErr != nil {
			fmt.Println(closeErr)
		}
	}()

	flow := runtime.From("async-double", exampleDouble)

	runID, submitErr := flow.Submit(context.Background(), 21, "order-21")
	if submitErr != nil {
		fmt.Println(submitErr)

		return
	}

	// Persist runID in application state; it can retrieve the result later.
	result, getErr := flow.Get(context.Background(), runID)
	if getErr != nil {
		fmt.Println(getErr)

		return
	}

	fmt.Println(result)
	// Output: 42
}

func ExamplePermanent() {
	database := exampledb.DB()

	runtime, err := cord.New(context.Background(), database, cord.Options{
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  time.Millisecond,
	})
	if err != nil {
		fmt.Println(err)

		return
	}

	_, runErr := runtime.From("permanent-error", examplePermanent).Run(context.Background(), 1)
	if err := closeExample(runtime, database); err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(runErr)
	// Output: invalid input: terminal
}

func ExampleOptions() {
	database := exampledb.DB()

	runtime, err := cord.New(context.Background(), database, cord.Options{
		Concurrency:       4,
		PollInterval:      100 * time.Millisecond,
		LeaseTTL:          30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		OnSchedulerError:  func(err error) { fmt.Println("scheduler:", err) },
	})
	if err != nil {
		fmt.Println(err)

		return
	}

	if err := runtime.Close(); err != nil {
		fmt.Println(err)

		return
	}

	// The caller still owns the database after the Cord runtime closes.
	if err := database.PingContext(context.Background()); err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println("database remains open")

	if err := database.Close(); err != nil {
		fmt.Println(err)
	}
	// Output: database remains open
}

func ExampleJoin() {
	database := exampledb.DB()

	runtime, err := cord.New(context.Background(), database)
	if err != nil {
		fmt.Println(err)

		return
	}

	root := runtime.From("order-total", exampleOrder)
	flow := cord.Join(root.Then(exampleItems), root.Then(exampleShipping)).Then(exampleTotal)

	total, runErr := flow.Run(context.Background(), 1001)
	if err := errors.Join(runErr, closeExample(runtime, database)); err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(total)
	// Output: 8
}
