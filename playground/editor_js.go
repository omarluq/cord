//go:build js && wasm

package playground

import (
	"fmt"
	"syscall/js"
)

type browserBridge struct {
	module js.Value
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

func (bridge browserBridge) mountGraph(elementID string) {
	element := js.Global().Get("document").Call("getElementById", elementID)
	bridge.module.Call("mountGraph", element)
}

func (bridge browserBridge) setGraph(message workerEvent) {
	bridge.module.Call("setGraph", message.value)
}

func (bridge browserBridge) setNodeState(identifier, state string) {
	bridge.module.Call("setNodeState", identifier, state)
}

func (bridge browserBridge) runWasm(bytes []byte, wasmExecURL string, receive func(workerEvent), exited func()) {
	array := js.Global().Get("Uint8Array").New(len(bytes))
	js.CopyBytesToJS(array, bytes)
	buffer := array.Get("buffer")

	onMessage := js.FuncOf(func(_ js.Value, arguments []js.Value) any {
		receive(workerEvent{value: arguments[0]})
		return nil
	})
	onExit := js.FuncOf(func(js.Value, []js.Value) any {
		exited()
		return nil
	})
	bridge.module.Call("runWasm", buffer, wasmExecURL, onMessage, onExit)
}

func (bridge browserBridge) stopWasm() {
	bridge.module.Call("stopWasm")
}
