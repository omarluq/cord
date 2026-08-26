package cord

import (
	"errors"
	"testing"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestSnapshotStorageErrorsPreserveBackendErrors(t *testing.T) {
	t.Parallel()

	backendErr := errors.New("backend unavailable")
	store := &snapshotStore{runErr: backendErr, nodeErr: backendErr}
	runtime := openSnapshotRuntime(store)

	_, err := runtime.InspectRun(t.Context(), snapshotRunID)
	require.ErrorIs(t, err, backendErr)
	_, err = runtime.ListRunNodes(t.Context(), snapshotRunID, NodeQuery{})
	require.ErrorIs(t, err, backendErr)
}

func openSnapshotRuntime(store storage.Backend) *Cord {
	return &Cord{store: store, acceptingRuns: true}
}
