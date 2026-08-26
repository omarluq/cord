package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strconv"
	"strings"

	"github.com/omarluq/cord/playground/internal/protocol"
)

func instrumentWorkflow(source string, graph protocol.Graph) (string, error) {
	if len(graph.Nodes) == 0 {
		return source, nil
	}

	fileSet := token.NewFileSet()

	file, err := parser.ParseFile(fileSet, "main.go", source, parser.ParseComments)
	if err != nil {
		return "", fmt.Errorf("parse workflow instrumentation: %w", err)
	}

	steps := make(map[string]string, len(graph.Nodes))
	ambiguous := make(map[string]struct{})

	for _, node := range graph.Nodes {
		name := stepBaseName(node.ID)
		if existing, found := steps[name]; found && existing != node.ID {
			delete(steps, name)
			ambiguous[name] = struct{}{}

			continue
		}

		if _, found := ambiguous[name]; !found {
			steps[name] = node.ID
		}
	}

	for _, declaration := range file.Decls {
		instrumentStep(declaration, steps)
	}

	var output bytes.Buffer
	if err := format.Node(&output, fileSet, file); err != nil {
		return "", fmt.Errorf("format workflow instrumentation: %w", err)
	}

	return output.String(), nil
}

func instrumentStep(declaration ast.Decl, steps map[string]string) {
	function, isFunction := declaration.(*ast.FuncDecl)
	if !isFunction || function.Body == nil {
		return
	}

	identifier, isStep := steps[function.Name.Name]
	if !isStep {
		return
	}

	errorResult, hasStepResults := nameStepResults(function)
	if !hasStepResults {
		return
	}

	function.Body.List = append([]ast.Stmt{
		nodeStateStatement(identifier, "running"),
		nodeCompletionStatement(identifier, errorResult),
	}, function.Body.List...)
}

func nodeStateStatement(identifier, state string) ast.Stmt {
	message := "__CORD_NODE__:" + identifier + ":" + state

	return &ast.ExprStmt{X: &ast.CallExpr{
		Fun: ast.NewIdent("println"),
		Args: []ast.Expr{&ast.BasicLit{
			Kind:  token.STRING,
			Value: strconv.Quote(message),
		}},
	}}
}

func nameStepResults(function *ast.FuncDecl) (string, bool) {
	if function.Type.Results == nil {
		return "", false
	}

	const resultCount = 2

	resultNames := make([]string, 0, resultCount)

	for index, field := range function.Type.Results.List {
		if len(field.Names) == 0 {
			field.Names = []*ast.Ident{ast.NewIdent(fmt.Sprintf("__cordResult%d", index))}
		}

		for _, name := range field.Names {
			if name.Name == "_" {
				name.Name = fmt.Sprintf("__cordResult%d", len(resultNames))
			}

			resultNames = append(resultNames, name.Name)
		}
	}

	if len(resultNames) != resultCount || !isErrorResult(function.Type.Results.List) {
		return "", false
	}

	return resultNames[1], true
}

func stepBaseName(identifier string) string {
	if separator := strings.LastIndexByte(identifier, '.'); separator >= 0 {
		return identifier[separator+1:]
	}

	return identifier
}

func isErrorResult(results []*ast.Field) bool {
	last := results[len(results)-1]
	identifier, ok := last.Type.(*ast.Ident)

	return ok && identifier.Name == "error"
}

func nodeCompletionStatement(identifier, errorResult string) ast.Stmt {
	failed := nodeStateStatement(identifier, "failed")
	completed := nodeStateStatement(identifier, "completed")
	body := &ast.BlockStmt{List: []ast.Stmt{
		&ast.IfStmt{
			Cond: &ast.BinaryExpr{X: ast.NewIdent(errorResult), Op: token.NEQ, Y: ast.NewIdent("nil")},
			Body: &ast.BlockStmt{List: []ast.Stmt{failed, &ast.ReturnStmt{}}},
		},
		completed,
	}}

	return &ast.DeferStmt{Call: &ast.CallExpr{Fun: &ast.FuncLit{
		Type: &ast.FuncType{Params: &ast.FieldList{}},
		Body: body,
	}}}
}
