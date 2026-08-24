package cord

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/gofrs/uuid/v5"

	"github.com/omarluq/cord/internal/storage"
	"github.com/omarluq/cord/internal/storage/sqlstore"
)

const (
	migrationTimeout      = 30 * time.Second
	heartbeatsPerLease    = 3
	minimumLeaseTTL       = 2 * time.Millisecond
	minimumLeasePrecision = time.Millisecond
)

var (
	// ErrSchemaOutdated indicates that the Cord schema is absent or older than required.
	ErrSchemaOutdated = errors.New("cord: schema is absent or outdated")
	// ErrSchemaNewer indicates that the Cord schema is newer than this Cord version.
	ErrSchemaNewer = errors.New("cord: schema is newer than runtime")
	// ErrMigrationFailed indicates that Cord could not inspect or migrate its schema.
	ErrMigrationFailed = errors.New("cord: migration failed")
)

// Options configures scheduler behavior. Zero-valued fields use Cord's
// defaults. Retry fields are defaulted independently, so callers may override
// any subset of them. LeaseTTL must be greater than two milliseconds,
// HeartbeatInterval must be at least one millisecond, and HeartbeatInterval
// must be less than half of LeaseTTL.
type Options struct {
	// OnSchedulerError reports scheduler storage errors serially from one runtime-
	// owned goroutine. A callback panic is recovered. The callback may call Close or
	// Shutdown; lifecycle waits exclude the callback because Go cannot cancel user
	// code. A callback that never returns therefore leaks that single reporter goroutine
	// and may outlive shutdown. At most 16 errors are queued while it is busy; later errors are
	// dropped and, if reporting resumes before shutdown, summarized by a later callback.
	// Shutdown abandons queued reports; one delivery already racing shutdown may begin.
	OnSchedulerError func(error)
	// Concurrency limits the number of nodes executing across all workflows.
	Concurrency int
	// PollInterval controls how often idle schedulers check for work.
	PollInterval time.Duration
	// LeaseTTL controls how long a worker owns a claimed node without a heartbeat.
	LeaseTTL time.Duration
	// HeartbeatInterval controls how often workers extend active leases.
	HeartbeatInterval time.Duration
	// MaxAttempts limits how many times each node may execute. Zero uses three.
	MaxAttempts int
	// RetryBaseDelay is the initial delay used for retry backoff. Zero uses
	// 500 milliseconds.
	RetryBaseDelay time.Duration
	// RetryMaxDelay caps retry backoff. Zero uses 30 seconds.
	RetryMaxDelay time.Duration
}

// New creates a workflow runtime using a caller-owned supported SQL database.
// It accepts at most one Options value and applies pending schema migrations.
func New(ctx context.Context, database *sql.DB, options ...Options) (*Cord, error) {
	if len(options) > 1 {
		return nil, errors.New("cord: New accepts at most one options value")
	}

	var opts Options
	if len(options) == 1 {
		opts = options[0]
	}

	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrMigrationFailed)
	}

	ctx, cancel := context.WithTimeout(ctx, migrationTimeout)
	defer cancel()

	if database == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrMigrationFailed)
	}

	settings, err := runtimeSettings(opts)
	if err != nil {
		return nil, err
	}

	store, err := sqlstore.New(ctx, database)
	if err != nil {
		return nil, publicMigrationError(err)
	}

	ownerID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("cord: generate runtime owner: %w", err)
	}

	return newCordWithSettings(store, ownerID.String(), settings), nil
}

func runtimeSettings(options Options) (schedulerSettings, error) {
	concurrency := options.Concurrency
	if concurrency == 0 {
		concurrency = max(1, runtime.GOMAXPROCS(0))
	}

	pollInterval := options.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}

	leaseTTL := options.LeaseTTL
	if leaseTTL == 0 {
		leaseTTL = defaultLeaseTTL
	}

	heartbeatInterval := options.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = max(
			minimumLeasePrecision,
			min(defaultHeartbeatInterval, leaseTTL/heartbeatsPerLease),
		)
	}

	retry, err := retrySettings(options)
	if err != nil {
		return schedulerSettings{}, err
	}

	if err := validateSchedulerSettings(concurrency, pollInterval, leaseTTL, heartbeatInterval); err != nil {
		return schedulerSettings{}, err
	}

	return schedulerSettings{
		concurrency:       concurrency,
		pollInterval:      pollInterval,
		leaseTTL:          leaseTTL,
		heartbeatInterval: heartbeatInterval,
		onSchedulerError:  options.OnSchedulerError,
		retry:             retry,
	}, nil
}

func validateSchedulerSettings(
	concurrency int,
	pollInterval time.Duration,
	leaseTTL time.Duration,
	heartbeatInterval time.Duration,
) error {
	if concurrency < 1 {
		return errors.New("cord: concurrency must be positive")
	}

	if pollInterval <= 0 {
		return errors.New("cord: poll interval must be positive")
	}

	if leaseTTL <= minimumLeaseTTL {
		return errors.New("cord: lease TTL must be greater than two milliseconds")
	}

	if heartbeatInterval < minimumLeasePrecision || heartbeatInterval >= leaseTTL-heartbeatInterval {
		return errors.New("cord: heartbeat interval must be at least one millisecond and less than half the lease TTL")
	}

	return nil
}

func retrySettings(options Options) (retryPolicy, error) {
	policy := defaultRetryPolicy()
	if options.MaxAttempts != 0 {
		policy.maxAttempts = options.MaxAttempts
	}

	if options.RetryBaseDelay != 0 {
		policy.baseDelay = options.RetryBaseDelay
	}

	if options.RetryMaxDelay != 0 {
		policy.maxDelay = options.RetryMaxDelay
	}

	if err := policy.validate(); err != nil {
		return retryPolicy{}, err
	}

	return policy, nil
}

func publicMigrationError(err error) error {
	switch {
	case errors.Is(err, storage.ErrSchemaOutdated):
		return fmt.Errorf("%w: %w", ErrSchemaOutdated, err)
	case errors.Is(err, storage.ErrSchemaNewer):
		return fmt.Errorf("%w: %w", ErrSchemaNewer, err)
	default:
		return fmt.Errorf("%w: %w", ErrMigrationFailed, err)
	}
}
