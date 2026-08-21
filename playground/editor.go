//go:build !js || !wasm

package playground

import "github.com/omarluq/cord/playground/internal/protocol"

type browserBridge struct{}

type workerEvent struct {
	typeName string
	id       string
	state    string
	value    string
	message  string
}

func (event *workerEvent) Type() string    { return event.typeName }
func (event *workerEvent) ID() string      { return event.id }
func (event *workerEvent) State() string   { return event.state }
func (event *workerEvent) Value() string   { return event.value }
func (event *workerEvent) Message() string { return event.message }

func loadBridge(done func(browserBridge, error)) {
	// Native tests do not load browser resources.
	done(browserBridge{}, nil)
}

func (browserBridge) mountEditor(string, string) {
	// Native tests do not mount browser resources.
}

func (browserBridge) source() string { return "" }

func (browserBridge) setSource(string) {
	// Native tests do not update browser resources.
}

func (browserBridge) mountGraph(string) {
	// Native tests do not mount browser resources.
}

func (browserBridge) setGraph(protocol.Graph) {
	// Native tests do not update browser resources.
}

func (browserBridge) zoomGraph(int) {
	// Native tests do not update browser resources.
}

func (browserBridge) setGraphState(string) {
	// Native tests do not update browser resources.
}

func (browserBridge) setNodeState(string, string) {
	// Native tests do not update browser resources.
}

func (browserBridge) runWasm([]byte, string, func(*workerEvent), func()) {
	// Native tests do not execute browser WebAssembly.
}

func (browserBridge) stopWasm() {
	// Native tests do not execute browser WebAssembly.
}

func (browserBridge) destroy() {
	// Native tests do not mount browser resources.
}
