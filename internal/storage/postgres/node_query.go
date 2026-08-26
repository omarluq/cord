package postgres

import (
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

func normalizeNodeQuery(query storage.NodeQuery) (int, error) {
	if err := validateNodeFilters(query); err != nil {
		return 0, err
	}

	if query.PageSize < 0 || query.PageSize > maxNodePageSize {
		return 0, fmt.Errorf("page size must be between 0 and %d", maxNodePageSize)
	}

	if query.PageSize == 0 {
		return defaultNodePageSize, nil
	}

	return query.PageSize, nil
}

func validateNodeFilters(query storage.NodeQuery) error {
	if query.State != nil && !query.State.IsKnown() {
		return fmt.Errorf("unknown node state filter %q", *query.State)
	}

	if query.Reason != nil && !query.Reason.IsKnown() {
		return fmt.Errorf("unknown node reason filter %q", *query.Reason)
	}

	if query.State != nil && query.Reason != nil && !query.State.AllowsReason(*query.Reason) {
		return fmt.Errorf("node state %q does not allow reason %q", *query.State, *query.Reason)
	}

	return nil
}

func nodeStateFilter(state *storage.NodeStatus) any {
	if state == nil {
		return nil
	}

	return *state
}

func nodeReasonFilter(reason *storage.TerminalReason) any {
	if reason == nil {
		return nil
	}

	return *reason
}
