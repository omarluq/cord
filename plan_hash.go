package cord

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/gofrs/uuid/v5"
	"github.com/omarluq/cord/internal/hashframe"
	"github.com/omarluq/cord/internal/storage"
)

const (
	planVersion                  = "cord/run-plan/v2"
	submissionFingerprintVersion = "cord/submission-fingerprint/v1"
)

func definitionHash(
	name string,
	inputFingerprint string,
	terminal storage.NodeID,
	nodes []storage.Node,
	edges []storage.Edge,
	retry retryPolicy,
) string {
	parents := make(map[storage.NodeID][]storage.Edge, len(nodes))
	for _, edge := range edges {
		parents[edge.Child] = append(parents[edge.Child], edge)
	}

	sorted := append([]storage.Node{}, nodes...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left].ID < sorted[right].ID })

	parts := []string{
		planVersion,
		"definition",
		name,
		inputFingerprint,
		string(terminal),
		strconv.Itoa(retryPolicyVersion),
		strconv.Itoa(retry.maxAttempts),
		strconv.FormatInt(retry.baseDelay.Nanoseconds(), 10),
		strconv.FormatInt(retry.maxDelay.Nanoseconds(), 10),
	}

	for index := range sorted {
		current := &sorted[index]
		currentParents := parents[current.ID]
		sort.Slice(currentParents, func(left, right int) bool {
			if currentParents[left].ParentOrder != currentParents[right].ParentOrder {
				return currentParents[left].ParentOrder < currentParents[right].ParentOrder
			}

			return currentParents[left].Parent < currentParents[right].Parent
		})

		parts = append(parts, string(current.ID), current.FunctionKey, current.SignatureHash)
		for _, edge := range currentParents {
			parts = append(parts, strconv.Itoa(edge.ParentOrder), string(edge.Parent))
		}
	}

	return hashframe.SHA256(parts...)
}

func submissionFingerprint(definitionHash string, input storage.EncodedPayload) string {
	return hashframe.SHA256(
		submissionFingerprintVersion,
		"submission",
		definitionHash,
		string(input),
	)
}

func generateRunID() (storage.RunID, error) {
	identifier, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("cord: generate run ID: %w", err)
	}

	return storage.RunID(identifier.String()), nil
}
