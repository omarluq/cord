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

var (
	// ErrSchemaOutdated indicates that the Cord schema is absent or older than required.
	ErrSchemaOutdated = errors.New("cord: schema is absent or outdated")
	// ErrSchemaNewer indicates that the Cord schema is newer than this Cord version.
	ErrSchemaNewer = errors.New("cord: schema is newer than runtime")
	// ErrMigrationFailed indicates that Cord could not inspect or migrate its schema.
	ErrMigrationFailed = errors.New("cord: migration failed")
)

// RuntimeOptions configures advanced scheduler behavior. Zero-valued fields use
// Cord's defaults. HeartbeatInterval must be shorter than LeaseTTL.
type RuntimeOptions struct {
	Concurrency       int
	PollInterval      time.Duration
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
}

// New creates a workflow runtime using a caller-owned SQLite database.
// It applies pending Cord schema migrations before returning.
func New(database *sql.DB) (*Cord, error) {
	return NewWithOptions(database, RuntimeOptions{})
}

// NewWithOptions creates a workflow runtime with advanced scheduler settings.
func NewWithOptions(database *sql.DB, options RuntimeOptions) (*Cord, error) {
	if database == nil {
		return nil, fmt.Errorf("%w: database is nil", ErrMigrationFailed)
	}

	err := storage.Migrate(context.Background(), database)
	if err != nil {
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

	concurrency, pollInterval, leaseTTL, heartbeatInterval, err := runtimeSettings(options)
	if err != nil {
		return nil, err
	}

	return newCordWithSettings(concurrency, store, ownerID.String(), schedulerSettings{
		pollInterval:      pollInterval,
		leaseTTL:          leaseTTL,
		heartbeatInterval: heartbeatInterval,
	}), nil
}

func runtimeSettings(options RuntimeOptions) (
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
		heartbeatInterval = defaultHeartbeatInterval
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
