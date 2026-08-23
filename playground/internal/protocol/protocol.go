// Package protocol defines messages exchanged by the playground compiler and UI.
// It contains only wire shapes and identifiers; HTTP and multipart processing
// belong to the client and server packages.
package protocol

const (
	// JSONMediaType is the media type for compile requests and error responses.
	JSONMediaType = "application/json"
	// MultipartMediaType is the media type for successful compile responses.
	MultipartMediaType = "multipart/form-data"
	// GraphPartName is the multipart field containing the compiled workflow graph.
	GraphPartName = "graph"
	// WASMPartName is the multipart field containing the WebAssembly artifact.
	WASMPartName = "wasm"
)

// CompileRequest requests compilation of Go source.
type CompileRequest struct {
	Source string `json:"source"`
}

// ErrorResponse describes a compiler error response.
type ErrorResponse struct {
	Error string `json:"error"`
}

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
