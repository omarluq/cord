package cord

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/omarluq/cord/internal/hashframe"
	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
)

type nodeDefinition struct {
	err         error
	logicalID   storage.NodeID
	functionKey string
	signature   string
}

func stepDefinition[I, O any](step func(context.Context, I) (O, error)) nodeDefinition {
	inputFingerprint, inputErr := typeFingerprint[I]()
	outputFingerprint, outputErr := typeFingerprint[O]()

	return newNodeDefinition(step, []string{inputFingerprint}, outputFingerprint, errors.Join(inputErr, outputErr))
}

func joinDefinition[A, B, O any](step func(context.Context, A, B) (O, error)) nodeDefinition {
	leftFingerprint, leftErr := typeFingerprint[A]()
	rightFingerprint, rightErr := typeFingerprint[B]()
	outputFingerprint, outputErr := typeFingerprint[O]()

	return newNodeDefinition(
		step,
		[]string{leftFingerprint, rightFingerprint},
		outputFingerprint,
		errors.Join(leftErr, rightErr, outputErr),
	)
}

func typeFingerprint[T any]() (string, error) {
	codec, err := serialization.NewJSONCodec[T]()
	if err != nil {
		return "", err
	}

	fingerprint, err := codec.TypeFingerprint()
	if err != nil {
		return "", fmt.Errorf("cord: fingerprint persistent type: %w", err)
	}

	return fingerprint, nil
}

func newNodeDefinition(step any, inputs []string, output string, codecErr error) nodeDefinition {
	functionKey, identityErr := functionKey(step)
	if err := errors.Join(identityErr, codecErr); err != nil {
		return nodeDefinition{err: err}
	}

	return nodeDefinition{
		functionKey: functionKey,
		signature:   serialization.SignatureFingerprint(inputs, output),
	}
}

func functionKey(step any) (string, error) {
	programCounter := reflect.ValueOf(step).Pointer()

	function := runtime.FuncForPC(programCounter)
	if function == nil {
		return "", errors.New("cord: workflow step has no persistent function identity")
	}

	name := function.Name()

	shortName := name[strings.LastIndex(name, "/")+1:]
	if strings.Contains(shortName, "[") {
		return "", fmt.Errorf("cord: generic workflow step %q is not supported", name)
	}

	generatedClosure := strings.Contains(shortName, ".func")

	methodWrapper := strings.HasSuffix(shortName, "-fm")
	if generatedClosure || methodWrapper || strings.Count(shortName, ".") != 1 {
		return "", fmt.Errorf("cord: workflow step %q is not a named package-level function", name)
	}

	return name, nil
}

func assignLogicalID(definition nodeDefinition, parents []node, occurrence int) nodeDefinition {
	if definition.err != nil {
		return definition
	}

	parts := []string{planVersion, "node", definition.functionKey, strconv.Itoa(occurrence)}
	for _, parent := range parents {
		parts = append(parts, string(parent.definition.logicalID))
	}

	definition.logicalID = storage.NodeID(hashframe.SHA256(parts...))

	return definition
}
