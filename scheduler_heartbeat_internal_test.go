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

func TestCord_HeartbeatPermitExhaustionIsReportedOnce(t *testing.T) {
	t.Parallel()

	runtime := &Cord{
		ctx:              t.Context(),
		heartbeatCalls:   make(chan struct{}, 1),
		errorReports:     make(chan error, 2),
		onSchedulerError: func(error) {},
	}
	runtime.heartbeatCalls <- struct{}{}

	state := &heartbeatState{}

	runtime.startHeartbeatCall(t.Context(), nil, state)
	runtime.startHeartbeatCall(t.Context(), nil, state)

	select {
	case report := <-runtime.errorReports:
		require.ErrorContains(t, report, "heartbeat call capacity exhausted")
	default:
		t.Fatal("heartbeat permit exhaustion was not reported")
	}

	require.Empty(t, runtime.errorReports)
}

func TestCord_HeartbeatFailureCancelsBeforeLeaseExpiry(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		heartbeatErr error
		name         string
		wantReport   bool
	}{
		{name: "lease rejected"},
		{name: "storage errors persist", heartbeatErr: errors.New("heartbeat unavailable"), wantReport: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			called := make(chan struct{}, 1)
			reports := make(chan error, 1)
			backend := heartbeatTestBackend{accepted: false, err: testCase.heartbeatErr, called: called}
			runtime := &Cord{
				store: backend, heartbeatInterval: 50 * time.Millisecond, leaseTTL: 200 * time.Millisecond,
				onSchedulerError: nonblockingErrorCallback(reports),
			}
			startErrorReporterForTest(t, runtime)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan bool, 1)
			claim := &storage.Claim{
				RunID: "", NodeID: "", FunctionKey: "", SignatureHash: "",
				Lease: storage.Lease{Owner: "", Generation: 0, Remaining: 200 * time.Millisecond,
					ExpiresAt: time.Now().Add(time.Hour)},
				Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
			}

			go runtime.heartbeat(ctx, claim, cancel, done)

			<-called

			select {
			case held := <-done:
				require.False(t, held)
			case <-time.After(time.Second):
				t.Fatal("heartbeat did not stop before the test deadline")
			}

			if testCase.wantReport {
				select {
				case report := <-reports:
					require.ErrorContains(t, report, testCase.heartbeatErr.Error())
				case <-time.After(time.Second):
					t.Fatal("heartbeat error was not reported")
				}
			} else {
				require.Empty(t, reports)
			}
		})
	}
}

func TestCord_HeartbeatUsesDatabaseRelativeLifetimeRegardlessOfWallClock(t *testing.T) {
	t.Parallel()

	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		t.Run(skew.String(), func(t *testing.T) {
			t.Parallel()

			called := make(chan struct{}, 1)
			backend := heartbeatTestBackend{
				accepted: true, remaining: 200 * time.Millisecond, called: called,
			}
			runtime := &Cord{
				store: backend, heartbeatInterval: 20 * time.Millisecond, leaseTTL: 200 * time.Millisecond,
			}
			ctx, stop := context.WithCancel(t.Context())
			done := make(chan bool, 1)
			claim := &storage.Claim{
				RunID: "", NodeID: "", FunctionKey: "", SignatureHash: "",
				Lease: storage.Lease{
					ExpiresAt: time.Now().Add(skew), Owner: "", Generation: 0, Remaining: 80 * time.Millisecond,
				},
				Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
			}

			go runtime.heartbeat(ctx, claim, stop, done)

			select {
			case <-called:
			case <-time.After(time.Second):
				t.Fatal("heartbeat followed wall-clock expiry instead of relative lifetime")
			}

			stop()
			require.True(t, <-done)
		})
	}
}

func TestCord_HeartbeatAcceptedBoundaryIsConservative(t *testing.T) {
	t.Parallel()

	called := make(chan struct{}, 1)
	backend := heartbeatTestBackend{accepted: true, remaining: 20 * time.Millisecond, called: called}
	runtime := &Cord{store: backend, heartbeatInterval: 20 * time.Millisecond, leaseTTL: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan bool, 1)
	claim := &storage.Claim{
		RunID: "", NodeID: "", FunctionKey: "", SignatureHash: "",
		Lease: storage.Lease{
			ExpiresAt: time.Time{}, Owner: "", Generation: 0, Remaining: 100 * time.Millisecond,
		},
		Attempt: 0, MaxAttempts: 0, RetryBaseDelay: 0, RetryMaxDelay: 0, RetryPolicyVersion: 0,
	}

	go runtime.heartbeat(ctx, claim, cancel, done)

	<-called
	require.False(t, <-done)
}
