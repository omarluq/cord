// Package protocol defines messages exchanged by the playground compiler and UI.
package protocol

const (
	// ArtifactMediaType identifies a multipart compiler response containing graph metadata and WebAssembly.
	ArtifactMediaType = "multipart/mixed"
	// GraphMediaType identifies the workflow graph part of a compiler response.
	GraphMediaType = "application/vnd.cord.graph+json"
	// WASMMediaType identifies the WebAssembly part of a compiler response.
	WASMMediaType = "application/wasm"
)

// Graph describes a workflow DAG extracted from submitted Go source.
type Graph struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

// Node describes one workflow step.
type Node struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// Edge describes a dependency between workflow steps.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}
