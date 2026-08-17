//go:build js && wasm

package client

import (
	"encoding/json"
	"fmt"
	"syscall/js"
)

func emit(eventType string, payload any) {
	envelope := struct {
		Type    string `json:"type"`
		Payload any    `json:"payload"`
	}{Type: eventType, Payload: payload}

	encoded, err := json.Marshal(envelope)
	if err != nil {
		panic(fmt.Sprintf("encode playground event: %v", err))
	}
	js.Global().Call("postMessage", string(encoded))
}
