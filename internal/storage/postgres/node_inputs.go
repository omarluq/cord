package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/omarluq/cord/internal/storage"
)

const parentOutputsQuery = `SELECT parent.output_payload
	FROM cord_edges edge
	JOIN cord_nodes parent
		ON parent.run_id = edge.run_id AND parent.node_id = edge.parent_node_id
	WHERE edge.run_id = $1 AND edge.child_node_id = $2
	ORDER BY edge.parent_order`

// LoadNodeInputs loads ordered parent outputs, or the run input for a root node.
func (s *Store) LoadNodeInputs(
	ctx context.Context,
	runID storage.RunID,
	nodeID storage.NodeID,
) (_ []storage.EncodedPayload, err error) {
	rows, err := s.pool.QueryContext(ctx, parentOutputsQuery, runID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("load parent outputs: %w", err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()

	inputs := make([]storage.EncodedPayload, 0)

	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan parent output: %w", err)
		}

		inputs = append(inputs, payload)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate parent outputs: %w", err)
	}

	if len(inputs) > 0 {
		return inputs, nil
	}

	const runInputQuery = `SELECT input_payload FROM cord_runs WHERE id = $1`

	var payload []byte
	if err = s.pool.QueryRowContext(ctx, runInputQuery, runID).Scan(&payload); err != nil {
		return nil, fmt.Errorf("load run input: %w", err)
	}

	return []storage.EncodedPayload{payload}, nil
}
