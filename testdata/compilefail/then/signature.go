package compilefail

import (
	"context"

	"github.com/omarluq/cord"
)

func incompatibleThenSignature() {
	flow := cord.New().From("then", func(_ context.Context, value int) (int, error) {
		return value, nil
	})

	flow.Then(func(_ context.Context, value string) (string, error) {
		return value, nil
	})
}
