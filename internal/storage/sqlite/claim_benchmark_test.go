package sqlite_test

import (
	"errors"
	"fmt"
	"github.com/omarluq/cord/internal/storage"
	cordsqlite "github.com/omarluq/cord/internal/storage/sqlite"
	"testing"
	"time"
)

// BenchmarkStore_PollingAndMaintenance measures idle polling and maintenance operations.
func BenchmarkStore_PollingAndMaintenance(b *testing.B) {
	benchmarks := []struct {
		run  func(*cordsqlite.Store) error
		name string
	}{
		{
			name: "empty claim",
			run: func(store *cordsqlite.Store) error {
				_, _, err := store.ClaimReadyNode(b.Context(), "benchmark-worker", time.Minute)
				if err != nil {
					return fmt.Errorf("claim ready node: %w", err)
				}

				return nil
			},
		},
		{
			name: "promote zero due",
			run: func(store *cordsqlite.Store) error {
				_, err := store.PromoteRetries(b.Context())
				if err != nil {
					return fmt.Errorf("promote retries: %w", err)
				}

				return nil
			},
		},
		{
			name: "recover zero expired",
			run: func(store *cordsqlite.Store) error {
				_, err := store.RecoverExpiredLeases(b.Context())
				if err != nil {
					return fmt.Errorf("recover expired leases: %w", err)
				}

				return nil
			},
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			store := newBenchmarkStore(b)
			b.ReportAllocs()

			for range b.N {
				if err := benchmark.run(store); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStore_RegisteredEmptyClaim measures empty claims across registration counts.
func BenchmarkStore_RegisteredEmptyClaim(b *testing.B) {
	for _, registrationCount := range []int{1, 10, 1000} {
		b.Run(fmt.Sprintf("registrations=%d", registrationCount), func(b *testing.B) {
			store := newBenchmarkStore(b)

			registrations := make([]storage.FunctionRegistration, registrationCount)
			for index := range registrationCount {
				registrations[index] = storage.FunctionRegistration{
					Key: fmt.Sprintf("function-%04d", index), Signature: fmt.Sprintf("signature-%04d", index),
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, claimed, claimErr := store.ClaimReadyNodeForFunctions(
					b.Context(), "benchmark-worker", time.Minute, registrations,
				)
				if claimErr != nil {
					b.Fatal(claimErr)
				}

				if claimed {
					b.Fatal("unexpected claim from empty database")
				}
			}
		})
	}
}

// BenchmarkStore_ClaimAndTransitions measures claiming and common node transitions.
func BenchmarkStore_ClaimAndTransitions(b *testing.B) {
	benchmarks := []struct {
		run          func(*testing.B, *cordsqlite.Store, *storage.Claim) error
		name         string
		includeClaim bool
	}{
		{name: "claim", includeClaim: true, run: noopBenchmarkTransition},
		{name: "complete", includeClaim: false, run: completeBenchmarkTransition},
		{name: "heartbeat", includeClaim: false, run: heartbeatBenchmarkTransition},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			benchmarkTransitions(b, benchmark.includeClaim, benchmark.run)
		})
	}
}

func benchmarkTransitions(
	b *testing.B,
	includeClaim bool,
	transition func(*testing.B, *cordsqlite.Store, *storage.Claim) error,
) {
	b.Helper()
	store := newBenchmarkStore(b)
	b.ReportAllocs()

	for index := range b.N {
		b.StopTimer()

		plan := benchmarkLinearPlan(storage.RunID(fmt.Sprintf("transition-%d", index)), 1)
		if err := store.CreateRun(b.Context(), &plan); err != nil {
			b.Fatal(err)
		}

		if includeClaim {
			b.StartTimer()
		}

		claim, claimed, err := store.ClaimReadyNode(b.Context(), "benchmark-worker", time.Minute)
		if err != nil || !claimed {
			b.Fatalf("claim benchmark node: claimed=%t err=%v", claimed, err)
		}

		if !includeClaim {
			b.StartTimer()
		}

		if err := transition(b, store, claim); err != nil {
			b.Fatal(err)
		}
	}
}

func noopBenchmarkTransition(b *testing.B, _ *cordsqlite.Store, _ *storage.Claim) error {
	b.Helper()

	return nil
}

func completeBenchmarkTransition(b *testing.B, store *cordsqlite.Store, claim *storage.Claim) error {
	b.Helper()

	accepted, err := store.CompleteNode(b.Context(), claim.RunID, claim.NodeID, claim.Lease, []byte(`1`))
	if err != nil {
		return fmt.Errorf("complete node: %w", err)
	}

	if !accepted {
		return errors.New("completion rejected")
	}

	return nil
}

func heartbeatBenchmarkTransition(b *testing.B, store *cordsqlite.Store, claim *storage.Claim) error {
	b.Helper()

	accepted, _, err := store.HeartbeatNode(b.Context(), claim.RunID, claim.NodeID, claim.Lease, time.Minute)
	if err != nil {
		return fmt.Errorf("heartbeat node: %w", err)
	}

	if !accepted {
		return errors.New("heartbeat rejected")
	}

	return nil
}
