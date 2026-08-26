package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"

	"github.com/omarluq/cord/playground/internal/protocol"
)

func extractGraph(source string) (protocol.Graph, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", source, 0)
	if err != nil {
		return protocol.Graph{}, fmt.Errorf("parse workflow graph: %w", err)
	}

	builder := graphBuilder{
		graph:     protocol.Graph{Nodes: []protocol.Node{}, Edges: []protocol.Edge{}},
		known:     make(map[string]struct{}),
		variables: make(map[string][]string),
		edges:     make(map[protocol.Edge]struct{}),
	}
	ast.Inspect(file, builder.inspect)

	return builder.graph, nil
}

type graphBuilder struct {
	known     map[string]struct{}
	variables map[string][]string
	edges     map[protocol.Edge]struct{}
	graph     protocol.Graph
}

func (builder *graphBuilder) inspect(node ast.Node) bool {
	if assignment, ok := node.(*ast.AssignStmt); ok {
		for index, value := range assignment.Rhs {
			terminals := builder.resolve(value)

			if index < len(assignment.Lhs) {
				if identifier, ok := assignment.Lhs[index].(*ast.Ident); ok {
					builder.variables[identifier.Name] = terminals
				}
			}
		}

		return false
	}

	call, isCall := node.(*ast.CallExpr)
	if !isCall {
		return true
	}

	selector, isSelector := call.Fun.(*ast.SelectorExpr)
	if !isSelector || (selector.Sel.Name != "From" && selector.Sel.Name != "Then" && selector.Sel.Name != "Join") {
		return true
	}

	builder.resolve(call)

	return false
}

func (builder *graphBuilder) resolve(expression ast.Expr) []string {
	switch value := expression.(type) {
	case *ast.Ident:
		return builder.variables[value.Name]
	case *ast.ParenExpr:
		return builder.resolve(value.X)
	case *ast.CallExpr:
		return builder.resolveCall(value)
	default:
		return nil
	}
}

func (builder *graphBuilder) resolveCall(call *ast.CallExpr) []string {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}

	switch selector.Sel.Name {
	case "From":
		return builder.resolveFrom(call)
	case "Then":
		return builder.resolveThen(selector.X, call)
	case "Join":
		return builder.resolveJoin(call)
	default:
		return nil
	}
}

func (builder *graphBuilder) resolveFrom(call *ast.CallExpr) []string {
	const stepArgumentCount = 2

	if len(call.Args) < stepArgumentCount {
		return nil
	}

	step := expressionName(call.Args[1])
	builder.addNode(step)

	return []string{step}
}

func (builder *graphBuilder) resolveThen(receiver ast.Expr, call *ast.CallExpr) []string {
	if len(call.Args) == 0 {
		return nil
	}

	parents := builder.resolve(receiver)
	child := expressionName(call.Args[0])
	builder.addNode(child)

	for _, parent := range parents {
		builder.addEdge(parent, child)
	}

	return []string{child}
}

func (builder *graphBuilder) resolveJoin(call *ast.CallExpr) []string {
	terminals := make([]string, 0, len(call.Args))
	for _, argument := range call.Args {
		terminals = append(terminals, builder.resolve(argument)...)
	}

	return terminals
}

func (builder *graphBuilder) addEdge(from, to string) {
	if from == "" || to == "" {
		return
	}

	edge := protocol.Edge{From: from, To: to}
	if _, exists := builder.edges[edge]; exists {
		return
	}

	builder.edges[edge] = struct{}{}
	builder.graph.Edges = append(builder.graph.Edges, edge)
}

func (builder *graphBuilder) addNode(identifier string) {
	if identifier == "" {
		return
	}

	if _, exists := builder.known[identifier]; exists {
		return
	}

	builder.known[identifier] = struct{}{}
	builder.graph.Nodes = append(builder.graph.Nodes, protocol.Node{ID: identifier, Label: identifier})
}

func expressionName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		prefix := expressionName(value.X)
		if prefix == "" {
			return value.Sel.Name
		}

		return prefix + "." + value.Sel.Name
	case *ast.IndexExpr:
		return expressionName(value.X)
	case *ast.IndexListExpr:
		return expressionName(value.X)
	case *ast.ParenExpr:
		return expressionName(value.X)
	}

	return ""
}
