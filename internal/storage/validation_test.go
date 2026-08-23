package storage_test

import (
	"testing"

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
