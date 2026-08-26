package storage_test

import (
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/conformance"
)

func TestValidateRunPlan(t *testing.T) {
	t.Parallel()

	for _, testCase := range conformance.ValidationRunPlanTests() {
		t.Run(testCase.Name, func(t *testing.T) {
			t.Parallel()

			plan := conformance.ValidationJoinPlan("run")
			testCase.Mutate(&plan)

			err := storage.ValidateRunPlan(&plan)
			if testCase.WantErr == "" {
				if err != nil {
					t.Fatalf("ValidateRunPlan() error = %v", err)
				}

				return
			}

			if err == nil || err.Error() != testCase.WantErr {
				t.Fatalf("ValidateRunPlan() error = %v, want %q", err, testCase.WantErr)
			}
		})
	}
}

func TestValidateRunPlanRejectsNilPlan(t *testing.T) {
	t.Parallel()

	err := storage.ValidateRunPlan(nil)
	if err == nil || err.Error() != "validate run plan: plan is nil" {
		t.Fatalf("ValidateRunPlan(nil) error = %v", err)
	}
}

func TestValidateRunPlanAdditionalInvariants(t *testing.T) {
	t.Parallel()

	const missingNodeID = "missing"

	tests := []struct {
		mutate  func(*storage.RunPlan)
		name    string
		wantErr string
	}{
		{
			name:    "node run ID",
			mutate:  func(plan *storage.RunPlan) { plan.Nodes[0].RunID = "other" },
			wantErr: `validate run plan: node "left" has run ID "other"`,
		},
		{
			name:    "missing terminal node",
			mutate:  func(plan *storage.RunPlan) { plan.Run.TerminalNodeID = missingNodeID },
			wantErr: `validate run plan: terminal node "missing" does not exist`,
		},
		{
			name:    "edge run ID",
			mutate:  func(plan *storage.RunPlan) { plan.Edges[0].RunID = "other" },
			wantErr: `validate run plan: edge "left" -> "joined" has run ID "other"`,
		},
		{
			name:    "empty edge parent",
			mutate:  func(plan *storage.RunPlan) { plan.Edges[0].Parent = "" },
			wantErr: "validate run plan: edge parent node ID is empty",
		},
		{
			name:    "empty edge child",
			mutate:  func(plan *storage.RunPlan) { plan.Edges[0].Child = "" },
			wantErr: "validate run plan: edge child node ID is empty",
		},
		{
			name:    "missing edge parent",
			mutate:  func(plan *storage.RunPlan) { plan.Edges[0].Parent = missingNodeID },
			wantErr: `validate run plan: edge parent "missing" does not exist`,
		},
		{
			name:    "missing edge child",
			mutate:  func(plan *storage.RunPlan) { plan.Edges[0].Child = missingNodeID },
			wantErr: `validate run plan: edge child "missing" does not exist`,
		},
		{
			name:    "nonpositive attempts",
			mutate:  func(plan *storage.RunPlan) { plan.Run.MaxAttempts = 0 },
			wantErr: "validate run plan: retry policy maximum attempts must be positive: 0",
		},
		{
			name:    "nonpositive base delay",
			mutate:  func(plan *storage.RunPlan) { plan.Run.RetryBaseDelay = 0 },
			wantErr: "validate run plan: retry policy base delay must be positive: 0s",
		},
		{
			name: "maximum delay below base delay",
			mutate: func(plan *storage.RunPlan) {
				plan.Run.RetryBaseDelay = time.Second
				plan.Run.RetryMaxDelay = time.Millisecond
			},
			wantErr: "validate run plan: retry policy maximum delay 1ms must be at least base delay 1s",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			plan := conformance.ValidationJoinPlan("run")
			test.mutate(&plan)

			err := storage.ValidateRunPlan(&plan)
			if err == nil || err.Error() != test.wantErr {
				t.Fatalf("ValidateRunPlan() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}
