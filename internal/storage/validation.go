package storage

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	supportedRetryPolicyVersion = 1
	maxIdempotencyKeyBytes      = 255
)

// ValidateRunPlan verifies backend-neutral run graph and initial-state invariants.
func ValidateRunPlan(plan *RunPlan) error {
	if plan == nil {
		return errors.New("validate run plan: plan is nil")
	}

	if err := validateInitialLifecycleMetadata(plan); err != nil {
		return err
	}

	if err := validateInitialRun(&plan.Run); err != nil {
		return err
	}

	dependencies := make(map[NodeID]int, len(plan.Nodes))
	for index := range plan.Nodes {
		current := &plan.Nodes[index]
		if current.RunID != plan.Run.ID {
			return fmt.Errorf("validate run plan: node %q has run ID %q", current.ID, current.RunID)
		}

		if current.ID == "" {
			return errors.New("validate run plan: node ID is empty")
		}

		if _, exists := dependencies[current.ID]; exists {
			return fmt.Errorf("validate run plan: duplicate node %q", current.ID)
		}

		if err := validateInitialNode(current); err != nil {
			return err
		}

		dependencies[current.ID] = 0
	}

	if _, exists := dependencies[plan.Run.TerminalNodeID]; !exists {
		return fmt.Errorf("validate run plan: terminal node %q does not exist", plan.Run.TerminalNodeID)
	}

	return validateEdges(plan, dependencies)
}

func validateInitialLifecycleMetadata(plan *RunPlan) error {
	runVersioned, err := validateInitialRunLifecycleMetadata(&plan.Run)
	if err != nil {
		return err
	}

	for index := range plan.Nodes {
		node := &plan.Nodes[index]

		nodeVersioned, validationErr := validateInitialNodeLifecycleMetadata(node)
		if validationErr != nil {
			return validationErr
		}

		if nodeVersioned != runVersioned {
			return fmt.Errorf(
				"validate run plan: node %q lifecycle version must be consistently present with run lifecycle version",
				node.ID,
			)
		}
	}

	return nil
}

func validateInitialRunLifecycleMetadata(run *Run) (bool, error) {
	if run.LifecycleVersion != nil && *run.LifecycleVersion != LifecycleVersion1 {
		return false, fmt.Errorf(
			"validate run plan: unsupported run lifecycle version %d (want %d)",
			*run.LifecycleVersion,
			LifecycleVersion1,
		)
	}

	if run.StartedAt != nil {
		return false, errors.New("validate run plan: run start time must initially be unset")
	}

	if run.TerminalReason != nil {
		return false, errors.New("validate run plan: run terminal reason must initially be unset")
	}

	if run.TerminalRunnerID != nil {
		return false, errors.New("validate run plan: run terminal runner ID must initially be unset")
	}

	return run.LifecycleVersion != nil, nil
}

func validateInitialNodeLifecycleMetadata(node *Node) (bool, error) {
	if node.LifecycleVersion != nil && *node.LifecycleVersion != LifecycleVersion1 {
		return false, fmt.Errorf(
			"validate run plan: node %q has unsupported lifecycle version %d (want %d)",
			node.ID,
			*node.LifecycleVersion,
			LifecycleVersion1,
		)
	}

	if node.StateChangedAt != nil {
		return false, fmt.Errorf(
			"validate run plan: node %q state-change time must initially be unset",
			node.ID,
		)
	}

	if node.LastStartedAt != nil {
		return false, fmt.Errorf(
			"validate run plan: node %q last start time must initially be unset",
			node.ID,
		)
	}

	if node.LastRunnerID != nil {
		return false, fmt.Errorf(
			"validate run plan: node %q last runner ID must initially be unset",
			node.ID,
		)
	}

	if node.TerminalReason != nil {
		return false, fmt.Errorf(
			"validate run plan: node %q terminal reason must initially be unset",
			node.ID,
		)
	}

	return node.LifecycleVersion != nil, nil
}

func validateInitialRun(run *Run) error {
	if err := validateRunIdentifiers(run); err != nil {
		return err
	}

	if err := validateSubmissionIdentity(run); err != nil {
		return err
	}

	if err := validateRunPayloads(run); err != nil {
		return err
	}

	if run.Status != RunRunning {
		return fmt.Errorf("validate run plan: run must initially be %q", RunRunning)
	}

	if run.CreatedAt.IsZero() {
		return errors.New("validate run plan: run creation time is zero")
	}

	if run.UpdatedAt.IsZero() {
		return errors.New("validate run plan: run update time is zero")
	}

	return validateRetryPolicy(run)
}

func validateRunIdentifiers(run *Run) error {
	if run.ID == "" {
		return errors.New("validate run plan: run ID is empty")
	}

	if run.WorkflowName == "" {
		return errors.New("validate run plan: workflow name is empty")
	}

	if run.DefinitionHash == "" {
		return errors.New("validate run plan: definition hash is empty")
	}

	if run.TerminalNodeID == "" {
		return errors.New("validate run plan: terminal node ID is empty")
	}

	return nil
}

func validateSubmissionIdentity(run *Run) error {
	if run.IdempotencyKey == nil {
		if run.SubmissionFingerprint != nil {
			return errors.New("validate run plan: unkeyed run has a submission fingerprint")
		}

		return nil
	}

	key := *run.IdempotencyKey
	if key == "" {
		return errors.New("validate run plan: idempotency key is empty")
	}

	if !utf8.ValidString(key) {
		return errors.New("validate run plan: idempotency key is not valid UTF-8")
	}

	if strings.IndexByte(key, 0) >= 0 {
		return errors.New("validate run plan: idempotency key contains NUL")
	}

	if len(key) > maxIdempotencyKeyBytes {
		return fmt.Errorf(
			"validate run plan: idempotency key is longer than %d bytes",
			maxIdempotencyKeyBytes,
		)
	}

	if run.SubmissionFingerprint == nil || *run.SubmissionFingerprint == "" {
		return errors.New("validate run plan: keyed run submission fingerprint is empty")
	}

	return nil
}

func validateRunPayloads(run *Run) error {
	if run.Input == nil {
		return errors.New("validate run plan: run input is nil")
	}

	if run.Output != nil {
		return errors.New("validate run plan: run output must initially be unset")
	}

	if run.Error != nil {
		return errors.New("validate run plan: run error must initially be unset")
	}

	if run.CompletedAt != nil {
		return errors.New("validate run plan: run completion time must initially be unset")
	}

	return nil
}

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

	if run.RetryPolicyVersion != supportedRetryPolicyVersion {
		return fmt.Errorf(
			"validate run plan: unsupported retry policy version %d (want %d)",
			run.RetryPolicyVersion,
			supportedRetryPolicyVersion,
		)
	}

	return nil
}

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
