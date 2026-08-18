//go:build js && wasm

package playground

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/omarluq/cord/playground/internal/protocol"
)

type browserBridge struct {
	module    js.Value
	onMessage js.Func
	onExit    js.Func
}

type workerEvent struct {
	value js.Value
}

func (event workerEvent) Type() string  { return event.stringValue("type") }
func (event workerEvent) ID() string    { return event.stringValue("id") }
func (event workerEvent) State() string { return event.stringValue("state") }
func (event workerEvent) Value() string { return event.stringValue("value") }
func (event workerEvent) Message() string {
	return event.stringValue("message")
}

func (event workerEvent) stringValue(key string) string {
	value := event.value.Get(key)
	if value.IsUndefined() || value.IsNull() {
		return ""
	}
	return value.String()
}

func loadBridge(done func(browserBridge, error)) {
	module := js.Global().Get("CordPlayground")
	if module.IsUndefined() || module.IsNull() {
		done(browserBridge{}, fmt.Errorf("load browser modules: CordPlayground is unavailable"))
		return
	}

	done(browserBridge{module: module}, nil)
}

func (bridge browserBridge) mountEditor(elementID, source string) {
	element := js.Global().Get("document").Call("getElementById", elementID)
	bridge.module.Call("mountEditor", element, source)
}

func (bridge browserBridge) source() string {
	return bridge.module.Call("source").String()
}

func (bridge browserBridge) setSource(source string) {
	bridge.module.Call("setSource", source)
}

func (bridge browserBridge) mountGraph(elementID string) {
	element := js.Global().Get("document").Call("getElementById", elementID)
	bridge.module.Call("mountGraph", element)
}

func (bridge browserBridge) setGraph(graph protocol.Graph) {
	encoded, err := json.Marshal(graph)
	if err != nil {
		return
	}
	bridge.module.Call("setGraph", js.Global().Get("JSON").Call("parse", string(encoded)))
}

func (bridge browserBridge) zoomGraph(direction int) {
	bridge.module.Call("zoomGraph", direction)
}

func (bridge browserBridge) setGraphState(state string) {
	bridge.module.Call("setGraphState", state)
}

func (bridge browserBridge) setNodeState(identifier, state string) {
	bridge.module.Call("setNodeState", identifier, state)
}

func (bridge *browserBridge) runWasm(bytes []byte, wasmExecURL string, receive func(workerEvent), exited func()) {
	bridge.stopWasm()
	array := js.Global().Get("Uint8Array").New(len(bytes))
	js.CopyBytesToJS(array, bytes)
	buffer := array.Get("buffer")

	bridge.onMessage = js.FuncOf(func(_ js.Value, arguments []js.Value) any {
		receive(workerEvent{value: arguments[0]})
		return nil
	})
	bridge.onExit = js.FuncOf(func(js.Value, []js.Value) any {
		bridge.module.Call("stopWasm")
		bridge.releaseCallbacks()
		exited()
		return nil
	})
	bridge.module.Call(
		"runWasm",
		buffer,
		wasmExecURL,
		bridge.onMessage,
		bridge.onExit,
	)
}

func (bridge *browserBridge) stopWasm() {
	bridge.module.Call("stopWasm")
	bridge.releaseCallbacks()
}

func (bridge *browserBridge) releaseCallbacks() {
	if !bridge.onMessage.IsUndefined() {
		bridge.onMessage.Release()
		bridge.onMessage = js.Func{}
	}
	if !bridge.onExit.IsUndefined() {
		bridge.onExit.Release()
		bridge.onExit = js.Func{}
	}
}
