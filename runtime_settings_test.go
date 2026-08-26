package cord_test

import (
	"context"
	"testing"
	"time"

	"github.com/omarluq/cord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func increment(_ context.Context, value int) (int, error) {
	return value + 1, nil
}

func double(_ context.Context, value int) (int, error) {
	return value * 2, nil
}

const (
	heartbeatIntervalError = "heartbeat interval"
	leaseTTLError          = "lease TTL"
	maximumDelayError      = "maximum delay"
)

func TestNew_OptionsValidatesSchedulerSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, error string
		options     cord.Options
	}{
		{name: "negative concurrency", options: cord.Options{Concurrency: -1}, error: "concurrency"},
		{name: "negative poll interval", options: cord.Options{PollInterval: -1}, error: "poll interval"},
		{name: "negative lease TTL", options: cord.Options{LeaseTTL: -1}, error: leaseTTLError},
		{
			name: "sub-millisecond lease TTL", options: cord.Options{LeaseTTL: time.Millisecond - 1},
			error: leaseTTLError,
		},
		{name: "one-millisecond lease TTL", options: cord.Options{LeaseTTL: time.Millisecond}, error: leaseTTLError},
		{
			name: "two-millisecond lease TTL", options: cord.Options{LeaseTTL: 2 * time.Millisecond},
			error: leaseTTLError,
		},
		{name: "negative heartbeat", options: cord.Options{HeartbeatInterval: -1}, error: heartbeatIntervalError},
		{name: "sub-millisecond heartbeat", options: cord.Options{
			LeaseTTL: time.Second, HeartbeatInterval: time.Millisecond - 1,
		}, error: heartbeatIntervalError},
		{name: "heartbeat equals half lease", options: cord.Options{
			LeaseTTL: time.Second, HeartbeatInterval: 500 * time.Millisecond,
		}, error: heartbeatIntervalError},
		{name: "heartbeat exceeds half lease", options: cord.Options{
			LeaseTTL: time.Second, HeartbeatInterval: 501 * time.Millisecond,
		}, error: heartbeatIntervalError},
		{name: "heartbeat equals lease", options: cord.Options{
			LeaseTTL: time.Second, HeartbeatInterval: time.Second,
		}, error: heartbeatIntervalError},
		{name: "negative max attempts", options: cord.Options{MaxAttempts: -1}, error: "maximum attempts"},
		{name: "negative retry base delay", options: cord.Options{RetryBaseDelay: -1}, error: "base delay"},
		{name: "negative retry maximum delay", options: cord.Options{RetryMaxDelay: -1}, error: maximumDelayError},
		{name: "retry base exceeds default maximum", options: cord.Options{
			RetryBaseDelay: 31 * time.Second,
		}, error: maximumDelayError},
		{name: "retry maximum precedes default base", options: cord.Options{
			RetryMaxDelay: time.Millisecond,
		}, error: maximumDelayError},
		{name: "retry maximum precedes explicit base", options: cord.Options{
			RetryBaseDelay: time.Second, RetryMaxDelay: time.Millisecond,
		}, error: maximumDelayError},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runtime, err := cord.New(t.Context(), openSQLite(t), testCase.options)

			assert.Nil(t, runtime)
			require.ErrorContains(t, err, testCase.error)
		})
	}
}

func TestNew_OptionsRetryDefaultsIndependently(t *testing.T) {
	t.Parallel()

	const (
		defaultMaxAttempts = 3
		defaultBaseDelay   = 500 * time.Millisecond
		defaultMaxDelay    = 30 * time.Second
	)

	tests := []struct {
		name                string
		options             cord.Options
		maxAttempts         int
		baseDelay, maxDelay time.Duration
	}{
		{
			name:        "all zero",
			maxAttempts: defaultMaxAttempts, baseDelay: defaultBaseDelay, maxDelay: defaultMaxDelay,
		},
		{
			name: "max attempts only", options: cord.Options{MaxAttempts: 5},
			maxAttempts: 5, baseDelay: defaultBaseDelay, maxDelay: defaultMaxDelay,
		},
		{
			name: "base delay only", options: cord.Options{RetryBaseDelay: time.Second},
			maxAttempts: defaultMaxAttempts, baseDelay: time.Second, maxDelay: defaultMaxDelay,
		},
		{
			name: "maximum delay only", options: cord.Options{RetryMaxDelay: time.Minute},
			maxAttempts: defaultMaxAttempts, baseDelay: defaultBaseDelay, maxDelay: time.Minute,
		},
		{
			name: "max attempts and base delay", options: cord.Options{
				MaxAttempts: 5, RetryBaseDelay: time.Second,
			},
			maxAttempts: 5, baseDelay: time.Second, maxDelay: defaultMaxDelay,
		},
		{
			name: "max attempts and maximum delay", options: cord.Options{
				MaxAttempts: 5, RetryMaxDelay: time.Minute,
			},
			maxAttempts: 5, baseDelay: defaultBaseDelay, maxDelay: time.Minute,
		},
		{
			name: "base and maximum delays", options: cord.Options{
				RetryBaseDelay: time.Second, RetryMaxDelay: time.Minute,
			},
			maxAttempts: defaultMaxAttempts, baseDelay: time.Second, maxDelay: time.Minute,
		},
		{
			name: "fully specified", options: cord.Options{
				MaxAttempts: 5, RetryBaseDelay: time.Second, RetryMaxDelay: time.Minute,
			},
			maxAttempts: 5, baseDelay: time.Second, maxDelay: time.Minute,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assertRetryOptions(t, testCase.options, testCase.maxAttempts, testCase.baseDelay, testCase.maxDelay)
		})
	}
}

func assertRetryOptions(
	t *testing.T,
	options cord.Options,
	wantMaxAttempts int,
	wantBaseDelay, wantMaxDelay time.Duration,
) {
	t.Helper()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database, options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	_, err = runtime.From("retry-defaults", increment).Run(t.Context(), 1)
	require.NoError(t, err)

	var (
		maxAttempts             int
		baseDelayNS, maxDelayNS int64
	)
	require.NoError(t, database.QueryRowContext(t.Context(), `
		SELECT max_attempts, retry_base_delay_ns, retry_max_delay_ns
		FROM cord_runs
	`).Scan(&maxAttempts, &baseDelayNS, &maxDelayNS))

	assert.Equal(t, wantMaxAttempts, maxAttempts)
	assert.Equal(t, wantBaseDelay, time.Duration(baseDelayNS))
	assert.Equal(t, wantMaxDelay, time.Duration(maxDelayNS))
}

func TestNew_OptionsValidatesBeforeMigration(t *testing.T) {
	t.Parallel()

	database := openSQLite(t)
	runtime, err := cord.New(t.Context(), database, cord.Options{Concurrency: -1})

	assert.Nil(t, runtime)
	require.ErrorContains(t, err, "concurrency")
	assert.False(t, sqliteTableExists(t, database, "cord_schema_migrations"))
}

func TestNew_OptionsLeasePrecisionBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options cord.Options
	}{
		{name: "derived heartbeat", options: cord.Options{LeaseTTL: 30 * time.Millisecond}},
		{name: "clamped derived heartbeat", options: cord.Options{LeaseTTL: 2*time.Millisecond + time.Nanosecond}},
		{name: "one millisecond heartbeat", options: cord.Options{
			LeaseTTL: 2*time.Millisecond + time.Nanosecond, HeartbeatInterval: time.Millisecond,
		}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			runtime, err := cord.New(t.Context(), openSQLite(t), testCase.options)
			require.NoError(t, err)
			require.NoError(t, runtime.Close())
		})
	}
}
