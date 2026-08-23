package storage

import (
	"encoding/json"
	"time"
)

type persistedLeaseExpiryFailure struct {
	Time        time.Time `json:"time"`
	Message     string    `json:"message"`
	NodeID      string    `json:"node_id"`
	FunctionKey string    `json:"function_key"`
	Attempt     int       `json:"attempt"`
	Retryable   bool      `json:"retryable"`
}

// EncodeLeaseExpiryFailure encodes the terminal failure caused when a node's
// final claimed attempt expires. The shape matches Cord's persisted failure
// payload contract so callers receive a useful workflow error after recovery.
func EncodeLeaseExpiryFailure(nodeID NodeID, functionKey string, attempt int, at time.Time) EncodedPayload {
	failure := persistedLeaseExpiryFailure{
		Time:        at.UTC(),
		Message:     "cord: node lease expired after final attempt",
		NodeID:      string(nodeID),
		FunctionKey: functionKey,
		Attempt:     attempt,
		Retryable:   false,
	}

	payload, err := json.Marshal(failure)
	if err != nil {
		return EncodedPayload(`{"message":"cord: node lease expired after final attempt","retryable":false}`)
	}

	return EncodedPayload(payload)
}
