package sqlite

import (
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

func incompatibleRun(runID storage.RunID, format string, arguments ...any) error {
	return fmt.Errorf("inspect run %q: %s: %w", runID, fmt.Sprintf(format, arguments...), storage.ErrRunIncompatible)
}

func incompatibleNode(
	runID storage.RunID,
	nodeID storage.NodeID,
	format string,
	arguments ...any,
) error {
	return fmt.Errorf(
		"inspect node %q for run %q: %s: %w",
		nodeID, runID, fmt.Sprintf(format, arguments...), storage.ErrRunIncompatible,
	)
}
