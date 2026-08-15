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
)

const (
	migrationTimeout   = 30 * time.Second
	heartbeatsPerLease = 3
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
// defaults. HeartbeatInterval must be shorter than LeaseTTL.
type Options struct {
	// OnSchedulerError reports scheduler storage errors. The callback must return promptly.
	OnSchedulerError func(error)
	// Concurrency limits the number of nodes executing across all workflows.
	Concurrency int
	// PollInterval controls how often idle schedulers check for work.
	PollInterval time.Duration
	// LeaseTTL controls how long a worker owns a claimed node without a heartbeat.
	LeaseTTL time.Duration
	// HeartbeatInterval controls how often workers extend active leases.
	HeartbeatInterval time.Duration
}

// New creates a workflow runtime using a caller-owned SQLite database. It
// accepts at most one Options value and applies pending schema migrations.
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

	concurrency, pollInterval, leaseTTL, heartbeatInterval, err := runtimeSettings(opts)
	if err != nil {
		return nil, err
	}

	if err = storage.Migrate(ctx, database); err != nil {
		return nil, publicMigrationError(err)
	}

	store, err := storage.NewStore(database)
	if err != nil {
		return nil, publicMigrationError(err)
	}

	ownerID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("cord: generate runtime owner: %w", err)
	}

	return newCordWithSettings(concurrency, store, ownerID.String(), schedulerSettings{
		pollInterval:      pollInterval,
		leaseTTL:          leaseTTL,
		heartbeatInterval: heartbeatInterval,
		onSchedulerError:  opts.OnSchedulerError,
	}), nil
}

func runtimeSettings(options Options) (
	concurrency int,
	pollInterval time.Duration,
	leaseTTL time.Duration,
	heartbeatInterval time.Duration,
	err error,
) {
	concurrency = options.Concurrency
	if concurrency == 0 {
		concurrency = max(1, runtime.GOMAXPROCS(0))
	}

	pollInterval = options.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}

	leaseTTL = options.LeaseTTL
	if leaseTTL == 0 {
		leaseTTL = defaultLeaseTTL
	}

	heartbeatInterval = options.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = min(defaultHeartbeatInterval, leaseTTL/heartbeatsPerLease)
	}

	if concurrency < 1 {
		return 0, 0, 0, 0, errors.New("cord: concurrency must be positive")
	}

	if pollInterval <= 0 {
		return 0, 0, 0, 0, errors.New("cord: poll interval must be positive")
	}

	if leaseTTL <= 0 {
		return 0, 0, 0, 0, errors.New("cord: lease TTL must be positive")
	}

	if heartbeatInterval <= 0 || heartbeatInterval >= leaseTTL {
		return 0, 0, 0, 0, errors.New("cord: heartbeat interval must be positive and shorter than lease TTL")
	}

	return concurrency, pollInterval, leaseTTL, heartbeatInterval, nil
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
