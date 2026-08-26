package storage_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/omarluq/cord/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeLeaseExpiryFailure(t *testing.T) {
	t.Parallel()

	failureTime := time.Date(2025, time.January, 2, 3, 4, 5, 6, time.FixedZone("test", 2*60*60))
	payload := storage.EncodeLeaseExpiryFailure("node-1", "package/function", 3, failureTime)

	var failure struct {
		Time        time.Time `json:"time"`
		Message     string    `json:"message"`
		NodeID      string    `json:"node_id"`
		FunctionKey string    `json:"function_key"`
		Attempt     int       `json:"attempt"`
		Retryable   bool      `json:"retryable"`
	}
	require.NoError(t, json.Unmarshal(payload, &failure))
	assert.Equal(t, failureTime.UTC(), failure.Time)
	assert.Equal(t, "cord: node lease expired after final attempt", failure.Message)
	assert.Equal(t, "node-1", failure.NodeID)
	assert.Equal(t, "package/function", failure.FunctionKey)
	assert.Equal(t, 3, failure.Attempt)
	assert.False(t, failure.Retryable)
}
