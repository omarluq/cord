package cord

import (
	"context"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
)

const joinedInputCount = 2

type encodedInvocation func(context.Context, []storage.EncodedPayload) (storage.EncodedPayload, error)

type registeredInvocation struct {
	invoke    encodedInvocation
	signature string
}

func encodedStep[I, O any](step func(context.Context, I) (O, error)) encodedInvocation {
	inputCodec, inputErr := serialization.NewJSONCodec[I]()
	outputCodec, outputErr := serialization.NewJSONCodec[O]()
	codecErr := errors.Join(inputErr, outputErr)

	return func(ctx context.Context, inputs []storage.EncodedPayload) (storage.EncodedPayload, error) {
		if codecErr != nil {
			return nil, codecErr
		}

		if len(inputs) != 1 {
			return nil, errors.New("cord: invalid persistent workflow node input")
		}

		input, err := inputCodec.Decode(inputs[0])
		if err != nil {
			return nil, fmt.Errorf("cord: decode persistent workflow node input: %w", err)
		}

		output, err := step(ctx, input)
		if err != nil {
			return nil, err
		}

		payload, err := outputCodec.Encode(output)
		if err != nil {
			return nil, fmt.Errorf("cord: encode persistent workflow node output: %w", err)
		}

		return storage.EncodedPayload(payload), nil
	}
}

func encodedJoin[A, B, O any](step func(context.Context, A, B) (O, error)) encodedInvocation {
	leftCodec, leftErr := serialization.NewJSONCodec[A]()
	rightCodec, rightErr := serialization.NewJSONCodec[B]()
	outputCodec, outputErr := serialization.NewJSONCodec[O]()
	codecErr := errors.Join(leftErr, rightErr, outputErr)

	return func(ctx context.Context, inputs []storage.EncodedPayload) (storage.EncodedPayload, error) {
		if codecErr != nil {
			return nil, codecErr
		}

		if len(inputs) != joinedInputCount {
			return nil, errors.New("cord: invalid persistent joined workflow node input")
		}

		left, leftErr := leftCodec.Decode(inputs[0])

		right, rightErr := rightCodec.Decode(inputs[1])
		if err := errors.Join(leftErr, rightErr); err != nil {
			return nil, fmt.Errorf("cord: decode persistent joined workflow node input: %w", err)
		}

		output, err := step(ctx, left, right)
		if err != nil {
			return nil, err
		}

		payload, err := outputCodec.Encode(output)
		if err != nil {
			return nil, fmt.Errorf("cord: encode persistent joined workflow node output: %w", err)
		}

		return storage.EncodedPayload(payload), nil
	}
}

func (c *Cord) register(definition nodeDefinition, invoke encodedInvocation) error {
	if definition.err != nil {
		return definition.err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.registry[definition.functionKey]; ok {
		if existing.signature != definition.signature {
			return fmt.Errorf("cord: conflicting registration for %q", definition.functionKey)
		}

		return nil
	}

	c.registry[definition.functionKey] = registeredInvocation{invoke: invoke, signature: definition.signature}
	c.registrations = nil

	return nil
}
