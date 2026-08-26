// Package linear demonstrates composing workflow steps in a linear chain.
package linear

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/omarluq/cord"
)

const doubleFactor = 2

func increment(_ context.Context, value int) (int, error) { return value + 1, nil }
func double(_ context.Context, value int) (int, error)    { return value * doubleFactor, nil }
func formatResult(_ context.Context, value int) (string, error) {
	return fmt.Sprintf("result: %d", value), nil
}

// Run executes the linear example using a caller-owned database.
func Run(ctx context.Context, database *sql.DB, input int) (_ string, err error) {
	runtime, err := cord.New(ctx, database)
	if err != nil {
		return "", fmt.Errorf("create runtime: %w", err)
	}

	defer func() {
		err = errors.Join(err, runtime.Close())
	}()

	flow := runtime.From("format-result", increment).Then(double).Then(formatResult)

	result, err := flow.Run(ctx, input)
	if err != nil {
		return "", fmt.Errorf("run workflow: %w", err)
	}

	return result, nil
}
