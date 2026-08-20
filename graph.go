package cord

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type nodeID uint64

type node struct {
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

func (g *graph) appendNode(parents []nodeID, definition nodeDefinition) nodeID {
	g.mu.Lock()
	defer g.mu.Unlock()

	parentNodes := make([]node, 0, len(parents))
	for _, parent := range parents {
		parentNode, ok := g.nodes[parent]
		if !ok {
			definition.err = errors.Join(definition.err, fmt.Errorf(
				"cord: workflow graph references unknown parent node %d",
				parent,
			))

			continue
		}

		parentNodes = append(parentNodes, parentNode)
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
			definition: current.definition,
		})

		return nil
	}

	if err := visit(tail); err != nil {
		return nil, err
	}

	assignCompiledLogicalIDs(plan)

	return plan, nil
}

func assignCompiledLogicalIDs(plan []node) {
	compiled := make(map[nodeID]node, len(plan))
	for index := range plan {
		current := &plan[index]

		parents := make([]node, 0, len(current.parents))
		for _, parent := range current.parents {
			parents = append(parents, compiled[parent])
		}

		definition := assignLogicalID(*current.definition, parents, compiledOccurrence(plan, index))
		current.definition = &definition
		compiled[current.id] = *current
	}
}

func compiledOccurrence(plan []node, index int) int {
	occurrence := 0

	for previous := range index {
		if plan[previous].definition.functionKey == plan[index].definition.functionKey &&
			reflect.DeepEqual(plan[previous].parents, plan[index].parents) {
			occurrence++
		}
	}

	return occurrence
}
