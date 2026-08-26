package storage

import (
	"errors"
	"fmt"
)

const supportedRetryPolicyVersion = 1

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
	if plan.Run.StartedAt != nil {
		return errors.New("validate run plan: run start time must initially be unset")
	}

	if plan.Run.TerminalReason != nil {
		return errors.New("validate run plan: run terminal reason must initially be unset")
	}

	if plan.Run.TerminalRunnerID != nil {
		return errors.New("validate run plan: run terminal runner ID must initially be unset")
	}

	for index := range plan.Nodes {
		node := &plan.Nodes[index]
		if node.StateChangedAt != nil {
			return fmt.Errorf("validate run plan: node %q state-change time must initially be unset", node.ID)
		}

		if node.LastStartedAt != nil {
			return fmt.Errorf("validate run plan: node %q last start time must initially be unset", node.ID)
		}

		if node.LastRunnerID != nil {
			return fmt.Errorf("validate run plan: node %q last runner ID must initially be unset", node.ID)
		}

		if node.TerminalReason != nil {
			return fmt.Errorf("validate run plan: node %q terminal reason must initially be unset", node.ID)
		}
	}

	return nil
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
