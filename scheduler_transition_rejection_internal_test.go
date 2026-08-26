package cord

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite" // Register the SQLite database/sql driver for this package's tests.
)

func TestCord_RejectedFencedTransitionsAreClassified(t *testing.T) {
	t.Parallel()

	const staleOwner = "stale"

	claim := &storage.Claim{
		RunID: raceRunID, NodeID: raceNodeID, FunctionKey: "", SignatureHash: "",
		Lease:   storage.Lease{ExpiresAt: time.Time{}, Owner: staleOwner, Generation: 1, Remaining: 0},
		Attempt: 1, MaxAttempts: 3, RetryBaseDelay: time.Second, RetryMaxDelay: time.Second,
		RetryPolicyVersion: 0,
	}

	testCases := []struct {
		run         func(*Cord) error
		name        string
		transition  string
		status      storage.RunStatus
		wantMessage string
		wantClass   fencedTransitionClass
	}{
		{
			name: "completion versus expiry", transition: completeTransition, status: storage.RunRunning,
			wantClass: fencedTransitionLeaseLost, wantMessage: "lease ownership was lost",
			run: func(runtime *Cord) error {
				return runtime.completeClaim(t.Context(), claim, []byte(`"stale"`))
			},
		},
		{
			name: "failure versus cancellation", transition: failTransition, status: storage.RunCanceled,
			wantClass: fencedTransitionCancellationWon, wantMessage: "run cancellation already won",
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, Permanent(errors.New("stale failure")))
			},
		},
		{
			name: "retry versus recovery", transition: retryTransition, status: storage.RunRunning,
			wantClass: fencedTransitionLeaseLost, wantMessage: "lease ownership was lost",
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, errors.New("stale retry"))
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: *testRunResult(testCase.status, nil)}
			runtime := &Cord{store: backend}

			err := testCase.run(runtime)

			var rejected *fencedTransitionError
			require.ErrorAs(t, err, &rejected)
			require.Equal(t, testCase.wantClass, rejected.class)
			require.ErrorContains(t, err, testCase.wantMessage)
			require.Equal(t, testCase.transition, backend.transition)
		})
	}
}

func TestCord_ExpectedRejectedClaimTransitionsAreNotReported(t *testing.T) {
	t.Parallel()

	claim := &storage.Claim{
		RunID: raceRunID, NodeID: raceNodeID, FunctionKey: "", SignatureHash: "",
		Lease: storage.Lease{
			ExpiresAt: time.Time{}, Owner: raceOwner, Generation: 1, Remaining: 0,
		},
		Attempt: 1, MaxAttempts: 3, RetryBaseDelay: time.Second, RetryMaxDelay: time.Second,
		RetryPolicyVersion: 0,
	}

	testCases := []struct {
		run        func(*Cord) error
		name       string
		transition string
		status     storage.RunStatus
	}{
		{
			name: "cancellation versus completion", transition: completeTransition, status: storage.RunCanceled,
			run: func(runtime *Cord) error {
				return runtime.completeClaim(t.Context(), claim, []byte(`"result"`))
			},
		},
		{
			name: "cancellation versus failure", transition: failTransition, status: storage.RunCanceled,
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, Permanent(errors.New("failure")))
			},
		},
		{
			name: "cancellation versus retry", transition: retryTransition, status: storage.RunCanceling,
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, errors.New("retry"))
			},
		},
		{
			name: "durable completion versus completion", transition: completeTransition, status: storage.RunCompleted,
			run: func(runtime *Cord) error {
				return runtime.completeClaim(t.Context(), claim, []byte(`"result"`))
			},
		},
		{
			name: "durable failure versus retry", transition: retryTransition, status: storage.RunFailed,
			run: func(runtime *Cord) error {
				return runtime.handleFailure(t.Context(), claim, errors.New("retry"))
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: *testRunResult(testCase.status, nil)}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			runtime := &Cord{
				store: backend, ctx: ctx, errorReports: make(chan error, 1), onSchedulerError: func(error) {},
			}

			runtime.reportClaimTransitionError(testCase.run(runtime))

			require.Equal(t, testCase.transition, backend.transition)
			require.Empty(t, runtime.errorReports)
		})
	}
}

func TestCord_ExpectedRejectedClaimReleaseIsNotReported(t *testing.T) {
	t.Parallel()

	for _, status := range []storage.RunStatus{
		storage.RunCanceling, storage.RunCanceled, storage.RunCompleted, storage.RunFailed,
	} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			backend := &rejectedTransitionBackend{result: *testRunResult(status, nil)}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			runtime := &Cord{
				store: backend, ctx: ctx, errorReports: make(chan error, 1), onSchedulerError: func(error) {},
			}

			runtime.releaseClaim(&storage.Claim{
				RunID: raceRunID, NodeID: raceNodeID, FunctionKey: "", SignatureHash: "",
				Lease: storage.Lease{
					ExpiresAt: time.Time{}, Owner: raceOwner, Generation: 1, Remaining: 0,
				},
				Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0,
				RetryPolicyVersion: 0,
			}, errors.New("registration disappeared"))

			require.Equal(t, retryTransition, backend.transition)
			require.Empty(t, runtime.errorReports)
		})
	}
}

func TestCord_RejectedClaimReleaseAfterReclaimIsReported(t *testing.T) {
	t.Parallel()

	reports := make(chan error, 1)
	backend := &rejectedTransitionBackend{result: *testRunResult(storage.RunRunning, nil)}
	runtime := &Cord{
		store: backend, onSchedulerError: func(err error) { reports <- err },
	}
	startErrorReporterForTest(t, runtime)

	claim := &storage.Claim{
		RunID: "reclaimed-run", NodeID: "reclaimed-node", FunctionKey: "", SignatureHash: "",
		Lease:   storage.Lease{ExpiresAt: time.Time{}, Owner: "stale", Generation: 1, Remaining: 0},
		Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
	}

	runtime.releaseClaim(claim, errors.New("registration disappeared"))

	report := <-reports

	var rejected *fencedTransitionError
	require.ErrorAs(t, report, &rejected)
	require.Equal(t, fencedTransitionLeaseLost, rejected.class)
	require.ErrorContains(t, report, "claim release rejected: lease ownership was lost")
	require.Equal(t, retryTransition, backend.transition)
}
