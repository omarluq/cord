// Package join demonstrates joining two workflow branches.
package join

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord"
)

const (
	doubleFactor = 2
	addend       = 3
)

func loadReport(_ context.Context, value int) (int, error) { return value, nil }
func double(_ context.Context, value int) (int, error)     { return value * doubleFactor, nil }
func addThree(_ context.Context, value int) (int, error)   { return value + addend, nil }
func sum(_ context.Context, left, right int) (int, error)  { return left + right, nil }

// Run executes the join example using a caller-owned database.
func Run(ctx context.Context, database *sql.DB, input int) (_ int, err error) {
	runtime, err := cord.New(ctx, database)
	if err != nil {
		return 0, fmt.Errorf("create runtime: %w", err)
	}

	defer func() {
		err = errors.Join(err, runtime.Close())
	}()

	root := runtime.From("report-summary", loadReport)
	flow := cord.Join(root.Then(double), root.Then(addThree)).Then(sum)

	result, err := flow.Run(ctx, input)
	if err != nil {
		return 0, fmt.Errorf("run workflow: %w", err)
	}

	return result, nil
}
