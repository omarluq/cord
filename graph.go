package cord

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type nodeID uint64

type invocation func(context.Context, []any) (any, error)

type node struct {
	invoke     invocation
	definition *nodeDefinition
	parents    []nodeID
	id         nodeID
}

type graph struct {
	nodes  map[nodeID]node
	name   string
	mu     sync.RWMutex
	nextID nodeID
}

func newGraph(name string) *graph {
	return &graph{
		mu:     sync.RWMutex{},
		name:   name,
		nextID: 0,
		nodes:  map[nodeID]node{},
	}
}

func (g *graph) appendNode(parents []nodeID, invoke invocation, definition nodeDefinition) nodeID {
	g.mu.Lock()
	defer g.mu.Unlock()

	parentNodes := make([]node, 0, len(parents))
	for _, parent := range parents {
		parentNodes = append(parentNodes, g.nodes[parent])
	}

	occurrence := 0

	for _, existing := range g.nodes {
		if existing.definition.functionKey == definition.functionKey && reflect.DeepEqual(existing.parents, parents) {
			occurrence++
		}
	}

	definition = assignLogicalID(definition, parentNodes, occurrence)

	g.nextID++
	nodeIdentifier := g.nextID
	g.nodes[nodeIdentifier] = node{
		id:         nodeIdentifier,
		parents:    append([]nodeID{}, parents...),
		invoke:     invoke,
		definition: &definition,
	}

	return nodeIdentifier
}

func (g *graph) compile(tail nodeID) ([]node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	selected := map[nodeID]bool{}
	visiting := map[nodeID]bool{}
	plan := make([]node, 0, len(g.nodes))

	var visit func(nodeID) error

	visit = func(nodeIdentifier nodeID) error {
		if selected[nodeIdentifier] {
			return nil
		}

		if visiting[nodeIdentifier] {
			return errors.New("cord: workflow graph contains a cycle")
		}

		current, ok := g.nodes[nodeIdentifier]
		if !ok {
			return fmt.Errorf("cord: workflow graph references unknown node %d", nodeIdentifier)
		}

		visiting[nodeIdentifier] = true

		for _, parent := range current.parents {
			if err := visit(parent); err != nil {
				return err
			}
		}

		delete(visiting, nodeIdentifier)
		selected[nodeIdentifier] = true

		plan = append(plan, node{
			id:         current.id,
			parents:    append([]nodeID{}, current.parents...),
			invoke:     current.invoke,
			definition: current.definition,
		})

		return nil
	}

	if err := visit(tail); err != nil {
		return nil, err
	}

	return plan, nil
}

type typedValue[T any] struct {
	value T
}

func adaptStep[I, O any](step func(context.Context, I) (O, error)) invocation {
	return func(ctx context.Context, inputs []any) (any, error) {
		if len(inputs) != 1 {
			return nil, errors.New("cord: invalid workflow node input")
		}

		input, ok := inputAs[I](inputs[0])
		if !ok {
			return nil, errors.New("cord: invalid workflow node input")
		}

		output, err := step(ctx, input)

		return typedValue[O]{value: output}, err
	}
}

func adaptJoin[A, B, O any](step func(context.Context, A, B) (O, error)) invocation {
	const inputCount = 2

	return func(ctx context.Context, inputs []any) (any, error) {
		if len(inputs) != inputCount {
			return nil, errors.New("cord: invalid joined workflow node input")
		}

		left, leftOK := inputAs[A](inputs[0])
		right, rightOK := inputAs[B](inputs[1])

		if !leftOK || !rightOK {
			return nil, errors.New("cord: invalid joined workflow node input")
		}

		output, err := step(ctx, left, right)

		return typedValue[O]{value: output}, err
	}
}

func inputAs[T any](value any) (T, bool) {
	if wrapped, ok := value.(typedValue[T]); ok {
		return wrapped.value, true
	}

	var zero T

	if value == nil {
		kind := reflect.TypeFor[T]().Kind()
		nilable := kind == reflect.Chan || kind == reflect.Func || kind == reflect.Interface ||
			kind == reflect.Map || kind == reflect.Pointer || kind == reflect.Slice

		return zero, nilable
	}

	typed, ok := value.(T)

	return typed, ok
}
