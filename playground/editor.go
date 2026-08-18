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

func (event workerEvent) Type() string    { return event.typeName }
func (event workerEvent) ID() string      { return event.id }
func (event workerEvent) State() string   { return event.state }
func (event workerEvent) Value() string   { return event.value }
func (event workerEvent) Message() string { return event.message }

func loadBridge(done func(browserBridge, error))  { done(browserBridge{}, nil) }
func (browserBridge) mountEditor(string, string)  {}
func (browserBridge) source() string              { return "" }
func (browserBridge) setSource(string)            {}
func (browserBridge) mountGraph(string)           {}
func (browserBridge) setGraph(protocol.Graph)     {}
func (browserBridge) zoomGraph(int)               {}
func (browserBridge) setGraphState(string)        {}
func (browserBridge) setNodeState(string, string) {}
func (browserBridge) runWasm([]byte, string, func(workerEvent), func()) {
}
func (browserBridge) stopWasm() {}
