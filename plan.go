package cord

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
)

const planVersion = "cord/run-plan/v1"

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

	definition.logicalID = storage.NodeID(hashParts(parts...))

	return definition
}

func buildPlan[I any](name string, plan []node, tail nodeID, input I) (*storage.RunPlan, error) {
	codec, err := serialization.NewJSONCodec[I]()
	if err != nil {
		return nil, fmt.Errorf("cord: validate workflow input for persistence: %w", err)
	}

	payload, err := codec.Encode(input)
	if err != nil {
		return nil, fmt.Errorf("cord: encode workflow input for persistence: %w", err)
	}

	inputFingerprint, err := codec.TypeFingerprint()
	if err != nil {
		return nil, fmt.Errorf("cord: fingerprint workflow input for persistence: %w", err)
	}

	runID, err := generateRunID()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	nodes, edges, logicalByRuntimeID, err := topology(plan, runID, now)
	if err != nil {
		return nil, err
	}

	terminal := logicalByRuntimeID[tail]

	return &storage.RunPlan{
		Run: storage.Run{
			CreatedAt:      now,
			UpdatedAt:      now,
			CompletedAt:    nil,
			ID:             runID,
			WorkflowName:   name,
			DefinitionHash: definitionHash(name, inputFingerprint, terminal, nodes, edges),
			TerminalNodeID: terminal,
			Status:         storage.RunRunning,
			Input:          storage.EncodedPayload(payload),
			Output:         nil,
			Error:          nil,
		},
		Nodes: nodes,
		Edges: edges,
	}, nil
}

func topology(
	plan []node,
	runID storage.RunID,
	now time.Time,
) ([]storage.Node, []storage.Edge, map[nodeID]storage.NodeID, error) {
	nodes := make([]storage.Node, 0, len(plan))
	edges := make([]storage.Edge, 0, len(plan))
	logicalByRuntimeID := make(map[nodeID]storage.NodeID, len(plan))

	for _, current := range plan {
		if current.definition.err != nil {
			return nil, nil, nil, fmt.Errorf(
				"cord: validate persistent node %d: %w",
				current.id,
				current.definition.err,
			)
		}

		logicalByRuntimeID[current.id] = current.definition.logicalID

		status := storage.NodePending
		if len(current.parents) == 0 {
			status = storage.NodeReady
		}

		nodes = append(nodes, storage.Node{
			AvailableAt:   now,
			CompletedAt:   nil,
			StartedAt:     nil,
			SignatureHash: current.definition.signature,
			RunID:         runID,
			ID:            current.definition.logicalID,
			FunctionKey:   current.definition.functionKey,
			Status:        status,
			Lease:         storage.Lease{},
			Error:         nil,
			Output:        nil,
			RemainingDeps: len(current.parents),
			Attempt:       0,
		})

		for parentOrder, parent := range current.parents {
			edges = append(edges, storage.Edge{
				RunID:       runID,
				Parent:      logicalByRuntimeID[parent],
				Child:       current.definition.logicalID,
				ParentOrder: parentOrder,
			})
		}
	}

	return nodes, edges, logicalByRuntimeID, nil
}

func definitionHash(
	name string,
	inputFingerprint string,
	terminal storage.NodeID,
	nodes []storage.Node,
	edges []storage.Edge,
) string {
	parents := make(map[storage.NodeID][]string, len(nodes))
	for _, edge := range edges {
		parents[edge.Child] = append(parents[edge.Child], string(edge.Parent))
	}

	sorted := append([]storage.Node{}, nodes...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left].ID < sorted[right].ID })

	parts := []string{planVersion, "definition", name, inputFingerprint, string(terminal)}

	for index := range sorted {
		current := &sorted[index]
		currentParents := parents[current.ID]
		sort.Strings(currentParents)

		parts = append(parts, string(current.ID), current.FunctionKey, current.SignatureHash)
		parts = append(parts, currentParents...)
	}

	return hashParts(parts...)
}

func hashParts(parts ...string) string {
	var framed strings.Builder
	for _, part := range parts {
		framed.WriteString(strconv.Itoa(len(part)))
		framed.WriteByte(':')
		framed.WriteString(part)
	}

	digest := sha256.Sum256([]byte(framed.String()))

	return hex.EncodeToString(digest[:])
}

func generateRunID() (storage.RunID, error) {
	identifier, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("cord: generate run ID: %w", err)
	}

	return storage.RunID(identifier.String()), nil
}
