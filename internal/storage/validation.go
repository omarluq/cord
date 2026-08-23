package storage

import (
	"errors"
	"fmt"
)

// ValidateRunPlan verifies backend-neutral run graph and initial-state invariants.
func ValidateRunPlan(plan *RunPlan) error {
	if plan == nil {
		return errors.New("validate run plan: plan is nil")
	}

	if plan.Run.ID == "" {
		return errors.New("validate run plan: run ID is empty")
	}

	if err := validateRetryPolicy(&plan.Run); err != nil {
		return err
	}

	dependencies := make(map[NodeID]int, len(plan.Nodes))
	for index := range plan.Nodes {
		current := &plan.Nodes[index]
		if current.RunID != plan.Run.ID {
			return fmt.Errorf("validate run plan: node %q has run ID %q", current.ID, current.RunID)
		}

		if _, exists := dependencies[current.ID]; exists {
			return fmt.Errorf("validate run plan: duplicate node %q", current.ID)
		}

		dependencies[current.ID] = 0
	}

	if _, exists := dependencies[plan.Run.TerminalNodeID]; !exists {
		return fmt.Errorf("validate run plan: terminal node %q does not exist", plan.Run.TerminalNodeID)
	}

	return validateEdges(plan, dependencies)
}

func validateRetryPolicy(run *Run) error {
	if run.MaxAttempts < 1 {
		return fmt.Errorf("validate run plan: retry policy maximum attempts must be positive: %d", run.MaxAttempts)
	}

	if run.RetryBaseDelay <= 0 {
		return fmt.Errorf("validate run plan: retry policy base delay must be positive: %s", run.RetryBaseDelay)
	}

	if run.RetryMaxDelay < run.RetryBaseDelay {
		return fmt.Errorf(
			"validate run plan: retry policy maximum delay %s must be at least base delay %s",
			run.RetryMaxDelay,
			run.RetryBaseDelay,
		)
	}

	if run.RetryPolicyVersion < 1 {
		return fmt.Errorf("validate run plan: retry policy version must be positive: %d", run.RetryPolicyVersion)
	}

	return nil
}

type edgeKey struct {
	parent NodeID
	child  NodeID
}

func validateEdges(plan *RunPlan, dependencies map[NodeID]int) error {
	children := make(map[NodeID][]NodeID, len(plan.Nodes))
	edges := make(map[edgeKey]struct{}, len(plan.Edges))

	for _, edge := range plan.Edges {
		if err := validateEdge(plan.Run.ID, edge, dependencies, edges); err != nil {
			return err
		}

		dependencies[edge.Child]++
		children[edge.Parent] = append(children[edge.Parent], edge.Child)
	}

	if cyclic(dependencies, children) {
		return errors.New("validate run plan: edges contain a cycle")
	}

	for index := range plan.Nodes {
		current := &plan.Nodes[index]
		if current.RemainingDeps != dependencies[current.ID] {
			return fmt.Errorf("validate run plan: node %q dependency count does not match edges", current.ID)
		}

		expected := NodePending
		if current.RemainingDeps == 0 {
			expected = NodeReady
		}

		if current.Status != expected {
			return fmt.Errorf("validate run plan: node %q must initially be %q", current.ID, expected)
		}
	}

	return nil
}

func validateEdge(runID RunID, edge Edge, dependencies map[NodeID]int, edges map[edgeKey]struct{}) error {
	if edge.RunID != runID {
		return fmt.Errorf("validate run plan: edge %q -> %q has run ID %q", edge.Parent, edge.Child, edge.RunID)
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

	edges[key] = struct{}{}

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
