package compilefail

import (
	"context"

	"github.com/omarluq/cord"
)

func incompatibleThenSignature() {
	runtime, _ := cord.New(nil)
	flow := runtime.From("test-workflow", func(_ context.Context, value int) (int, error) {
		return value, nil
	})

	flow.Then(func(_ context.Context, value string) (string, error) {
		return value, nil
	})
}
