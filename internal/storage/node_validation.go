package storage

import "fmt"

func validateInitialNode(node *Node) error {
	if err := validateNodeIdentity(node); err != nil {
		return err
	}

	if err := validateNodePayloads(node); err != nil {
		return err
	}

	return validateNodeExecutionState(node)
}

func validateNodeIdentity(node *Node) error {
	if node.FunctionKey == "" {
		return fmt.Errorf("validate run plan: node %q function key is empty", node.ID)
	}

	if node.SignatureHash == "" {
		return fmt.Errorf("validate run plan: node %q signature hash is empty", node.ID)
	}

	if node.AvailableAt.IsZero() {
		return fmt.Errorf("validate run plan: node %q availability time is zero", node.ID)
	}

	return nil
}

func validateNodePayloads(node *Node) error {
	if node.Output != nil {
		return fmt.Errorf("validate run plan: node %q output must initially be unset", node.ID)
	}

	if node.Error != nil {
		return fmt.Errorf("validate run plan: node %q error must initially be unset", node.ID)
	}

	if node.StartedAt != nil {
		return fmt.Errorf("validate run plan: node %q start time must initially be unset", node.ID)
	}

	if node.CompletedAt != nil {
		return fmt.Errorf("validate run plan: node %q completion time must initially be unset", node.ID)
	}

	return nil
}

func validateNodeExecutionState(node *Node) error {
	if node.Lease.Owner != "" {
		return fmt.Errorf("validate run plan: node %q lease owner must initially be empty", node.ID)
	}

	if node.Lease.Generation != 0 {
		return fmt.Errorf("validate run plan: node %q lease generation must initially be zero", node.ID)
	}

	if !node.Lease.ExpiresAt.IsZero() {
		return fmt.Errorf("validate run plan: node %q lease expiry must initially be unset", node.ID)
	}

	if node.Lease.Remaining != 0 {
		return fmt.Errorf("validate run plan: node %q lease remaining time must initially be zero", node.ID)
	}

	if node.Attempt != 0 {
		return fmt.Errorf("validate run plan: node %q attempt must initially be zero", node.ID)
	}

	if node.RemainingDeps < 0 {
		return fmt.Errorf("validate run plan: node %q dependency count must be non-negative", node.ID)
	}

	return nil
}

func validateInitialNodeStates(nodes []Node, dependencies map[NodeID]int) error {
	for index := range nodes {
		current := &nodes[index]
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
