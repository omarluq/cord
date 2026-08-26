package conformance

import (
	"strings"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

func runIdentityValidationTests() []RunPlanValidationTest {
	return []RunPlanValidationTest{
		{
			Name: "valid",
			Mutate: func(*storage.RunPlan) {
				// The valid case intentionally leaves the plan unchanged.
			},
			WantErr: "",
		},
		{
			Name:    "empty run ID",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.ID = "" },
			WantErr: "validate run plan: run ID is empty",
		},
		{
			Name:    "empty workflow name",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.WorkflowName = "" },
			WantErr: "validate run plan: workflow name is empty",
		},
		{
			Name:    "empty definition hash",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.DefinitionHash = "" },
			WantErr: "validate run plan: definition hash is empty",
		},
		{
			Name:    "empty terminal node ID",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.TerminalNodeID = "" },
			WantErr: "validate run plan: terminal node ID is empty",
		},
	}
}

func idempotencyValidationTests() []RunPlanValidationTest {
	const overlongKeyLength = 256

	return []RunPlanValidationTest{
		{
			Name: "empty idempotency key",
			Mutate: func(plan *storage.RunPlan) {
				plan.Run.IdempotencyKey = new("")
				plan.Run.SubmissionFingerprint = new("fingerprint")
			},
			WantErr: "validate run plan: idempotency key is empty",
		},
		{
			Name: "invalid UTF-8 idempotency key",
			Mutate: func(plan *storage.RunPlan) {
				plan.Run.IdempotencyKey = new(string([]byte{0xff}))
				plan.Run.SubmissionFingerprint = new("fingerprint")
			},
			WantErr: "validate run plan: idempotency key is not valid UTF-8",
		},
		{
			Name: "idempotency key containing NUL",
			Mutate: func(plan *storage.RunPlan) {
				plan.Run.IdempotencyKey = new("key\x00suffix")
				plan.Run.SubmissionFingerprint = new("fingerprint")
			},
			WantErr: "validate run plan: idempotency key contains NUL",
		},
		{
			Name: "idempotency key longer than 255 bytes",
			Mutate: func(plan *storage.RunPlan) {
				plan.Run.IdempotencyKey = new(strings.Repeat("k", overlongKeyLength))
				plan.Run.SubmissionFingerprint = new("fingerprint")
			},
			WantErr: "validate run plan: idempotency key is longer than 255 bytes",
		},
		{
			Name: "missing keyed submission fingerprint",
			Mutate: func(plan *storage.RunPlan) {
				plan.Run.IdempotencyKey = new("key")
			},
			WantErr: "validate run plan: keyed run submission fingerprint is empty",
		},
		{
			Name: "empty keyed submission fingerprint",
			Mutate: func(plan *storage.RunPlan) {
				plan.Run.IdempotencyKey = new("key")
				plan.Run.SubmissionFingerprint = new("")
			},
			WantErr: "validate run plan: keyed run submission fingerprint is empty",
		},
		{
			Name: "fingerprint without idempotency key",
			Mutate: func(plan *storage.RunPlan) {
				plan.Run.SubmissionFingerprint = new("fingerprint")
			},
			WantErr: "validate run plan: unkeyed run has a submission fingerprint",
		},
	}
}

func lifecycleMetadataValidationTests() []RunPlanValidationTest {
	return lifecycleFieldValidationTests()
}

func lifecycleFieldValidationTests() []RunPlanValidationTest {
	now := time.Now().UTC()
	reason := storage.ReasonSucceeded
	runnerID := storage.RunnerID("runner")

	return []RunPlanValidationTest{
		{
			Name:    "initial run start",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.StartedAt = &now },
			WantErr: "validate run plan: run start time must initially be unset",
		},
		{
			Name:    "initial run terminal reason",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.TerminalReason = &reason },
			WantErr: "validate run plan: run terminal reason must initially be unset",
		},
		{
			Name:    "initial run terminal runner ID",
			Mutate:  func(plan *storage.RunPlan) { plan.Run.TerminalRunnerID = &runnerID },
			WantErr: "validate run plan: run terminal runner ID must initially be unset",
		},
		{
			Name:    "initial node state change",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].StateChangedAt = &now },
			WantErr: `validate run plan: node "left" state-change time must initially be unset`,
		},
		{
			Name:    "initial node last start",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].LastStartedAt = &now },
			WantErr: `validate run plan: node "left" last start time must initially be unset`,
		},
		{
			Name:    "initial node last runner ID",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].LastRunnerID = &runnerID },
			WantErr: `validate run plan: node "left" last runner ID must initially be unset`,
		},
		{
			Name:    "initial node terminal reason",
			Mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].TerminalReason = &reason },
			WantErr: `validate run plan: node "left" terminal reason must initially be unset`,
		},
	}
}
