// Package protocol defines messages exchanged by the playground compiler and UI.
package protocol

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
