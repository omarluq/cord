package cord

import (
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

const (
	defaultNodePageSize = 50
	maxNodePageSize     = 200
)

func normalizeNodeQuery(runID RunID, query NodeQuery) (NodeQuery, NodeID, error) {
	query, err := normalizeNodeFilters(query)
	if err != nil {
		return NodeQuery{}, "", err
	}

	if query.ContinuationToken == "" {
		return query, "", nil
	}

	token, err := decodeNodePageToken(query.ContinuationToken)
	if err != nil {
		return NodeQuery{}, "", fmt.Errorf("cord: invalid node continuation token: %w", err)
	}

	if token.RunID != runID || token.State != nodeStateFilter(query) || token.Reason != nodeReasonFilter(query) {
		return NodeQuery{}, "", errors.New("cord: node continuation token does not match run or filters")
	}

	query.ContinuationToken = ""

	return query, token.LastNodeID, nil
}

func normalizeNodeFilters(query NodeQuery) (NodeQuery, error) {
	if query.PageSize < 0 || query.PageSize > maxNodePageSize {
		return NodeQuery{}, fmt.Errorf("cord: node page size must be between 0 and %d", maxNodePageSize)
	}

	if query.PageSize == 0 {
		query.PageSize = defaultNodePageSize
	}

	if query.State != nil {
		state := *query.State
		if !state.IsKnown() {
			return NodeQuery{}, fmt.Errorf("cord: unknown node state %q", state)
		}

		query.State = &state
	}

	if query.Reason != nil {
		reason := *query.Reason
		if !reason.IsKnown() {
			return NodeQuery{}, fmt.Errorf("cord: unknown terminal reason %q", reason)
		}

		query.Reason = &reason
	}

	return query, nil
}

func storageNodeQuery(query NodeQuery, cursor NodeID) storage.NodeQuery {
	converted := storage.NodeQuery{
		State: nil, Reason: nil, ContinuationToken: string(cursor), PageSize: query.PageSize,
	}
	if query.State != nil {
		state := storage.NodeStatus(*query.State)
		converted.State = &state
	}

	if query.Reason != nil {
		reason := storage.TerminalReason(*query.Reason)
		converted.Reason = &reason
	}

	return converted
}

func nodeStateFilter(query NodeQuery) NodeState {
	if query.State == nil {
		return ""
	}

	return *query.State
}

func nodeReasonFilter(query NodeQuery) TerminalReason {
	if query.Reason == nil {
		return ""
	}

	return *query.Reason
}
