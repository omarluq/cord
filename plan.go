package cord

import (
	"fmt"
	"time"

	"github.com/omarluq/cord/internal/serialization"
	"github.com/omarluq/cord/internal/storage"
)

func buildPlan[I any](
	name string,
	plan []node,
	tail nodeID,
	input I,
	retry retryPolicy,
) (*storage.RunPlan, error) {
	codec, err := serialization.NewJSONCodec[I]()
	if err != nil {
		return nil, fmt.Errorf("cord: validate workflow input for persistence: %w", err)
	}

	payload, err := codec.Encode(input)
	if err != nil {
		return nil, fmt.Errorf("cord: encode workflow input for persistence: %w", err)
	}

	inputFingerprint, err := codec.TypeFingerprint()
	if err != nil {
		return nil, fmt.Errorf("cord: fingerprint workflow input for persistence: %w", err)
	}

	runID, err := generateRunID()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	nodes, edges, logicalByRuntimeID, err := topology(plan, runID, now)
	if err != nil {
		return nil, err
	}

	terminal := logicalByRuntimeID[tail]

	return &storage.RunPlan{
		Run: storage.Run{
			CreatedAt:             now,
			UpdatedAt:             now,
			CompletedAt:           nil,
			StartedAt:             nil,
			TerminalReason:        nil,
			TerminalRunnerID:      nil,
			ID:                    runID,
			WorkflowName:          name,
			DefinitionHash:        definitionHash(name, inputFingerprint, terminal, nodes, edges, retry),
			IdempotencyKey:        nil,
			SubmissionFingerprint: nil,
			TerminalNodeID:        terminal,
			Status:                storage.RunRunning,
			Input:                 storage.EncodedPayload(payload),
			Output:                nil,
			Error:                 nil,
			MaxAttempts:           retry.maxAttempts,
			RetryBaseDelay:        retry.baseDelay,
			RetryMaxDelay:         retry.maxDelay,
			RetryPolicyVersion:    retryPolicyVersion,
		},
		Nodes: nodes,
		Edges: edges,
	}, nil
}

func topology(
	plan []node,
	runID storage.RunID,
	now time.Time,
) ([]storage.Node, []storage.Edge, map[nodeID]storage.NodeID, error) {
	nodes := make([]storage.Node, 0, len(plan))
	edges := make([]storage.Edge, 0, len(plan))
	logicalByRuntimeID := make(map[nodeID]storage.NodeID, len(plan))

	for _, current := range plan {
		if current.definition.err != nil {
			return nil, nil, nil, fmt.Errorf(
				"cord: validate persistent node %d: %w",
				current.id,
				current.definition.err,
			)
		}

		logicalByRuntimeID[current.id] = current.definition.logicalID

		status := storage.NodePending
		if len(current.parents) == 0 {
			status = storage.NodeReady
		}

		nodes = append(nodes, storage.Node{
			AvailableAt:    now,
			CompletedAt:    nil,
			StartedAt:      nil,
			StateChangedAt: nil,
			LastStartedAt:  nil,
			LastRunnerID:   nil,
			TerminalReason: nil,
			SignatureHash:  current.definition.signature,
			RunID:          runID,
			ID:             current.definition.logicalID,
			FunctionKey:    current.definition.functionKey,
			Status:         status,
			Lease:          storage.Lease{},
			Error:          nil,
			Output:         nil,
			RemainingDeps:  len(current.parents),
			Attempt:        0,
		})

		for parentOrder, parent := range current.parents {
			edges = append(edges, storage.Edge{
				RunID:       runID,
				Parent:      logicalByRuntimeID[parent],
				Child:       current.definition.logicalID,
				ParentOrder: parentOrder,
			})
		}
	}

	return nodes, edges, logicalByRuntimeID, nil
}
