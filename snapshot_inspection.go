package cord

import (
	"context"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

// RunnerID returns the opaque identity of this runtime incarnation. The value
// is stable for a successfully created Cord instance and differs for a newly
// created instance. It is diagnostic metadata, not a secret, principal,
// authorization credential, metric label, or lease fencing token. A nil Cord
// has an empty RunnerID.
func (c *Cord) RunnerID() RunnerID {
	if c == nil {
		return ""
	}

	return RunnerID(c.owner)
}

// InspectRun returns one read-only, payload-free snapshot of id without waiting
// for the run to finish or reconstructing its workflow. It does not promote
// retries, recover leases, or authorize access: applications must enforce their
// own tenancy and authorization policy before calling it. Missing and malformed
// or unsupported runs return errors matching ErrRunNotFound and
// ErrRunIncompatible, respectively.
func (c *Cord) InspectRun(ctx context.Context, runID RunID) (RunReport, error) {
	if ctx == nil {
		return RunReport{}, errors.New("cord: snapshot context is nil")
	}

	if runID == "" {
		return RunReport{}, errors.New("cord: run ID is empty")
	}

	if err := c.validateSnapshotRuntime(); err != nil {
		return RunReport{}, err
	}

	report, err := c.store.InspectRun(ctx, storage.RunID(runID))
	if err != nil {
		return RunReport{}, snapshotStorageError("inspect run", runID, err)
	}

	converted, err := publicRunReport(&report)
	if err != nil {
		return RunReport{}, err
	}

	if converted.ID != runID {
		return RunReport{}, incompatibleSnapshot("storage returned run %q while inspecting %q", converted.ID, runID)
	}

	return converted, nil
}

// ListRunNodes returns one bounded, payload-free page ordered by stable NodeID.
// Continuation tokens are portable across Cord instances and replicas, but are
// not credentials and do not replace application authorization. A token is
// bound to the run and normalized filters. Pages may observe state changes made
// between calls and therefore do not provide cross-page snapshot isolation.
func (c *Cord) ListRunNodes(ctx context.Context, runID RunID, query NodeQuery) (NodePage, error) {
	if ctx == nil {
		return NodePage{}, errors.New("cord: snapshot context is nil")
	}

	if runID == "" {
		return NodePage{}, errors.New("cord: run ID is empty")
	}

	normalized, cursor, err := normalizeNodeQuery(runID, query)
	if err != nil {
		return NodePage{}, err
	}

	if runtimeErr := c.validateSnapshotRuntime(); runtimeErr != nil {
		return NodePage{}, runtimeErr
	}

	page, err := c.store.ListRunNodes(ctx, storage.RunID(runID), storageNodeQuery(normalized, cursor))
	if err != nil {
		return NodePage{}, snapshotStorageError("list run nodes", runID, err)
	}

	return publicNodePage(runID, normalized, page)
}

func (c *Cord) validateSnapshotRuntime() error {
	if c == nil || c.store == nil {
		return errors.New("cord: invalid runtime")
	}

	c.admissionMu.Lock()
	open := c.acceptingRuns
	c.admissionMu.Unlock()

	if !open {
		return errors.New("cord: runtime closed")
	}

	return nil
}

func snapshotStorageError(operation string, runID RunID, err error) error {
	switch {
	case errors.Is(err, storage.ErrRunNotFound):
		return fmt.Errorf("cord: %s %q: %w", operation, runID, ErrRunNotFound)
	case errors.Is(err, storage.ErrRunIncompatible):
		return fmt.Errorf("cord: %s %q: %w", operation, runID, ErrRunIncompatible)
	default:
		return fmt.Errorf("cord: %s %q: %w", operation, runID, err)
	}
}

func incompatibleSnapshot(format string, values ...any) error {
	return fmt.Errorf("cord: incompatible run snapshot: %s: %w", fmt.Sprintf(format, values...), ErrRunIncompatible)
}
