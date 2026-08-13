package cord_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/omarluq/cord"
)

const fuzzGraphMaxNodes = 32

type fuzzValue struct {
	number int64
	tail   int
}

type fuzzGraph struct {
	workflows []cord.Workflow[fuzzValue, fuzzValue]
	parents   [][]int
	values    []fuzzValue
	calls     []atomic.Int32
}

func FuzzWorkflow_GraphReachability(f *testing.F) {
	f.Add(int64(3), []byte{}, uint8(0))
	f.Add(int64(-7), []byte{0, 2, 5, 255}, uint8(2))
	f.Add(int64(11), []byte{1, 3, 7, 15, 31}, uint8(5))

	f.Fuzz(func(t *testing.T, input int64, instructions []byte, tailSelector uint8) {
		if len(instructions) >= fuzzGraphMaxNodes {
			instructions = instructions[:fuzzGraphMaxNodes-1]
		}

		graph := newFuzzGraph(input, instructions)
		tail := int(tailSelector) % len(graph.workflows)

		result, err := graph.workflows[tail].Run(t.Context(), fuzzValue{number: input, tail: 0})
		if err != nil {
			t.Fatalf("run generated acyclic graph: %v", err)
		}

		if result != graph.values[tail] {
			t.Fatalf("terminal result = %+v, want %+v", result, graph.values[tail])
		}

		assertFuzzGraphCalls(t, &graph, tail)
	})
}

func newFuzzGraph(input int64, instructions []byte) fuzzGraph {
	nodeCount := len(instructions) + 1
	graph := fuzzGraph{
		workflows: make([]cord.Workflow[fuzzValue, fuzzValue], nodeCount),
		parents:   make([][]int, nodeCount),
		values:    make([]fuzzValue, nodeCount),
		calls:     make([]atomic.Int32, nodeCount),
	}

	graph.workflows[0] = cord.New().From(
		"fuzz-reachability",
		func(_ context.Context, value fuzzValue) (fuzzValue, error) {
			graph.calls[0].Add(1)

			return value, nil
		},
	)
	graph.values[0] = fuzzValue{number: input, tail: 0}

	for index, instruction := range instructions {
		graph.appendInstruction(index+1, instruction)
	}

	return graph
}

func (g *fuzzGraph) appendInstruction(current int, instruction byte) {
	left := int(instruction>>1) % current
	if instruction&1 == 0 {
		g.parents[current] = []int{left}
		g.values[current] = fuzzValue{number: g.values[left].number + int64(current), tail: current}
		g.workflows[current] = g.workflows[left].Then(func(_ context.Context, value fuzzValue) (fuzzValue, error) {
			g.calls[current].Add(1)

			if value.tail != left {
				return fuzzValue{}, fmt.Errorf("node %d received tail %d, want %d", current, value.tail, left)
			}

			return fuzzValue{number: value.number + int64(current), tail: current}, nil
		})

		return
	}

	right := (int(instruction)*7 + current) % current
	g.parents[current] = []int{left, right}
	g.values[current] = fuzzValue{
		number: g.values[left].number + g.values[right].number + int64(current),
		tail:   current,
	}
	g.workflows[current] = cord.Join(g.workflows[left], g.workflows[right]).Then(
		func(_ context.Context, leftValue, rightValue fuzzValue) (fuzzValue, error) {
			g.calls[current].Add(1)

			if leftValue.tail != left || rightValue.tail != right {
				return fuzzValue{}, fmt.Errorf(
					"node %d received tails (%d, %d), want (%d, %d)",
					current, leftValue.tail, rightValue.tail, left, right,
				)
			}

			return fuzzValue{number: leftValue.number + rightValue.number + int64(current), tail: current}, nil
		},
	)
}

func assertFuzzGraphCalls(t *testing.T, graph *fuzzGraph, tail int) {
	t.Helper()

	reachable := fuzzReachableNodes(graph.parents, tail)
	for identifier := range graph.calls {
		wantCalls := int32(0)
		if reachable[identifier] {
			wantCalls = 1
		}

		if got := graph.calls[identifier].Load(); got != wantCalls {
			t.Fatalf("node %d calls = %d, want %d (tail %d)", identifier, got, wantCalls, tail)
		}
	}
}

func fuzzReachableNodes(parents [][]int, tail int) []bool {
	reachable := make([]bool, len(parents))
	pending := []int{tail}

	for len(pending) > 0 {
		last := len(pending) - 1
		identifier := pending[last]
		pending = pending[:last]

		if reachable[identifier] {
			continue
		}

		reachable[identifier] = true
		pending = append(pending, parents[identifier]...)
	}

	return reachable
}
