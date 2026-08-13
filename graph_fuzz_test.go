package cord_test

import "testing"

func FuzzWorkflow_Linear(f *testing.F) {
	f.Setenv("TMPDIR", f.TempDir())
	f.Add(3)
	f.Add(-7)
	f.Add(11)

	f.Fuzz(func(t *testing.T, input int) {
		flow := mustRuntime(t).From(addOne).Then(timesTwo)

		result, err := flow.Run(t.Context(), input)
		if err != nil {
			t.Fatalf("run workflow: %v", err)
		}

		if want := (input + 1) * 2; result != want {
			t.Fatalf("result = %d, want %d", result, want)
		}
	})
}
