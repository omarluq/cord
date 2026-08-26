package storage

import (
	"errors"
	"fmt"
)

type edgeKey struct {
	parent NodeID
	child  NodeID
}

func validateEdges(plan *RunPlan, dependencies map[NodeID]int) error {
	children := make(map[NodeID][]NodeID, len(plan.Nodes))
	parents := make(map[NodeID][]NodeID, len(plan.Nodes))
	edges := make(map[edgeKey]struct{}, len(plan.Edges))
	parentOrders := make(map[NodeID]map[int]struct{}, len(plan.Nodes))

	for _, edge := range plan.Edges {
		if err := validateEdge(plan.Run.ID, edge, dependencies, edges, parentOrders); err != nil {
			return err
		}

		dependencies[edge.Child]++
		children[edge.Parent] = append(children[edge.Parent], edge.Child)
		parents[edge.Child] = append(parents[edge.Child], edge.Parent)
	}

	if err := validateParentOrders(plan.Nodes, dependencies, parentOrders); err != nil {
		return err
	}

	if cyclic(dependencies, children) {
		return errors.New("validate run plan: edges contain a cycle")
	}

	if err := validateInitialNodeStates(plan.Nodes, dependencies); err != nil {
		return err
	}

	return validateTerminalAncestry(plan.Nodes, plan.Run.TerminalNodeID, parents)
}

func validateTerminalAncestry(nodes []Node, terminalNodeID NodeID, parents map[NodeID][]NodeID) error {
	ancestors := make(map[NodeID]struct{}, len(nodes))
	ancestors[terminalNodeID] = struct{}{}
	pending := []NodeID{terminalNodeID}

	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		for _, parent := range parents[current] {
			if _, visited := ancestors[parent]; visited {
				continue
			}

			ancestors[parent] = struct{}{}
			pending = append(pending, parent)
		}
	}

	for index := range nodes {
		if _, visited := ancestors[nodes[index].ID]; !visited {
			return fmt.Errorf(
				"validate run plan: node %q does not reach terminal node %q",
				nodes[index].ID,
				terminalNodeID,
			)
		}
	}

	return nil
}

func validateParentOrders(
	nodes []Node,
	dependencies map[NodeID]int,
	parentOrders map[NodeID]map[int]struct{},
) error {
	for index := range nodes {
		child := nodes[index].ID
		for expected := range dependencies[child] {
			if _, exists := parentOrders[child][expected]; !exists {
				return fmt.Errorf(
					"validate run plan: node %q parent order values must be contiguous from zero (missing %d)",
					child,
					expected,
				)
			}
		}
	}

	return nil
}

func validateEdge(
	runID RunID,
	edge Edge,
	dependencies map[NodeID]int,
	edges map[edgeKey]struct{},
	parentOrders map[NodeID]map[int]struct{},
) error {
	if edge.RunID != runID {
		return fmt.Errorf("validate run plan: edge %q -> %q has run ID %q", edge.Parent, edge.Child, edge.RunID)
	}

	if edge.Parent == "" {
		return errors.New("validate run plan: edge parent node ID is empty")
	}

	if edge.Child == "" {
		return errors.New("validate run plan: edge child node ID is empty")
	}

	if _, exists := dependencies[edge.Parent]; !exists {
		return fmt.Errorf("validate run plan: edge parent %q does not exist", edge.Parent)
	}

	if _, exists := dependencies[edge.Child]; !exists {
		return fmt.Errorf("validate run plan: edge child %q does not exist", edge.Child)
	}

	key := edgeKey{parent: edge.Parent, child: edge.Child}
	if _, exists := edges[key]; exists {
		return fmt.Errorf("validate run plan: duplicate edge %q -> %q", edge.Parent, edge.Child)
	}

	if edge.ParentOrder < 0 {
		return fmt.Errorf(
			"validate run plan: edge %q -> %q parent order must be non-negative",
			edge.Parent,
			edge.Child,
		)
	}

	orders := parentOrders[edge.Child]
	if orders == nil {
		orders = make(map[int]struct{})
		parentOrders[edge.Child] = orders
	}

	if _, exists := orders[edge.ParentOrder]; exists {
		return fmt.Errorf(
			"validate run plan: node %q has duplicate parent order %d",
			edge.Child,
			edge.ParentOrder,
		)
	}

	edges[key] = struct{}{}
	orders[edge.ParentOrder] = struct{}{}

	return nil
}

func cyclic(dependencies map[NodeID]int, children map[NodeID][]NodeID) bool {
	remaining := make(map[NodeID]int, len(dependencies))
	ready := make([]NodeID, 0, len(dependencies))

	for nodeID, dependencyCount := range dependencies {
		remaining[nodeID] = dependencyCount
		if dependencyCount == 0 {
			ready = append(ready, nodeID)
		}
	}

	visited := 0

	for len(ready) > 0 {
		current := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		visited++

		for _, child := range children[current] {
			remaining[child]--
			if remaining[child] == 0 {
				ready = append(ready, child)
			}
		}
	}

	return visited != len(dependencies)
}
