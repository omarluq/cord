package cord

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/omarluq/cord/internal/storage"
)

type persistedFailure struct {
	Time        time.Time `json:"time"`
	Message     string    `json:"message"`
	NodeID      string    `json:"node_id"`
	FunctionKey string    `json:"function_key"`
	Attempt     int       `json:"attempt"`
	Retryable   bool      `json:"retryable"`
}

type runFailureError struct{ failure *persistedFailure }

func (err runFailureError) Error() string { return err.failure.Message }

func encodeFailure(claim *storage.Claim, err error) storage.EncodedPayload {
	failure := persistedFailure{
		Message: err.Error(), NodeID: string(claim.NodeID), FunctionKey: claim.FunctionKey,
		Time: time.Now().UTC(), Attempt: claim.Attempt, Retryable: !isPermanent(err),
	}

	payload, marshalErr := json.Marshal(failure)
	if marshalErr != nil {
		return storage.EncodedPayload([]byte(`{"message":"cord: encode failure"}`))
	}

	return storage.EncodedPayload(payload)
}

func decodeRunError(payload storage.EncodedPayload) error {
	var failure persistedFailure
	if err := json.Unmarshal(payload, &failure); err != nil || failure.Message == "" {
		return errors.New("cord: workflow failed")
	}

	return runFailureError{failure: &failure}
}
