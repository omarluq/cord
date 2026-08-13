package compilefail

import (
	"context"

	"github.com/omarluq/cord"
)

func incompatibleJoinSignature() {
	runtime, _ := cord.New(nil)
	left := runtime.From(func(_ context.Context, value int) (string, error) {
		return string(rune(value)), nil
	})
	right := left.Then(func(_ context.Context, value string) (int, error) {
		return len(value), nil
	})

	cord.Join(left, right).Then(func(_ context.Context, leftValue int, rightValue string) (int, error) {
		return leftValue + len(rightValue), nil
	})
}
