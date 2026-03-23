package graph

import (
	"testing"
)

func TestTopologicalSortLinearChain(t *testing.T) {
	// C -> B -> A (A depends on B, B depends on C)
	dag := NewDAG()
	dag.AddNode(&Node{Name: "A", DependsOn: []string{"B"}})
	dag.AddNode(&Node{Name: "B", DependsOn: []string{"C"}})
	dag.AddNode(&Node{Name: "C"})

	sorted, err := dag.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 3 {
		t.Fatalf("got %d nodes, want 3", len(sorted))
	}

	// Build position map to verify ordering constraints.
	pos := make(map[string]int)
	for i, n := range sorted {
		pos[n.Name] = i
	}
	// C must come before B, B must come before A.
	if pos["C"] >= pos["B"] {
		t.Errorf("C (pos %d) should come before B (pos %d)", pos["C"], pos["B"])
	}
	if pos["B"] >= pos["A"] {
		t.Errorf("B (pos %d) should come before A (pos %d)", pos["B"], pos["A"])
	}
}

func TestTopologicalSortDiamond(t *testing.T) {
	// D has no deps; B depends on D; C depends on D; A depends on B and C.
	dag := NewDAG()
	dag.AddNode(&Node{Name: "A", DependsOn: []string{"B", "C"}})
	dag.AddNode(&Node{Name: "B", DependsOn: []string{"D"}})
	dag.AddNode(&Node{Name: "C", DependsOn: []string{"D"}})
	dag.AddNode(&Node{Name: "D"})

	sorted, err := dag.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 4 {
		t.Fatalf("got %d nodes, want 4", len(sorted))
	}

	pos := make(map[string]int)
	for i, n := range sorted {
		pos[n.Name] = i
	}
	if pos["D"] >= pos["B"] {
		t.Errorf("D should come before B")
	}
	if pos["D"] >= pos["C"] {
		t.Errorf("D should come before C")
	}
	if pos["B"] >= pos["A"] {
		t.Errorf("B should come before A")
	}
	if pos["C"] >= pos["A"] {
		t.Errorf("C should come before A")
	}
}

func TestTopologicalSortCycleDetection(t *testing.T) {
	dag := NewDAG()
	dag.AddNode(&Node{Name: "A", DependsOn: []string{"B"}})
	dag.AddNode(&Node{Name: "B", DependsOn: []string{"C"}})
	dag.AddNode(&Node{Name: "C", DependsOn: []string{"A"}})

	_, err := dag.TopologicalSort()
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

func TestTopologicalSortNoDependencies(t *testing.T) {
	dag := NewDAG()
	dag.AddNode(&Node{Name: "A"})
	dag.AddNode(&Node{Name: "B"})
	dag.AddNode(&Node{Name: "C"})

	sorted, err := dag.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 3 {
		t.Fatalf("got %d nodes, want 3", len(sorted))
	}
	// All nodes should appear (order is non-deterministic for independent nodes).
	names := make(map[string]bool)
	for _, n := range sorted {
		names[n.Name] = true
	}
	for _, name := range []string{"A", "B", "C"} {
		if !names[name] {
			t.Errorf("missing node %s in sorted output", name)
		}
	}
}

func TestSelectSubgraph(t *testing.T) {
	dag := NewDAG()
	dag.AddNode(&Node{Name: "A", DependsOn: []string{"B"}})
	dag.AddNode(&Node{Name: "B", DependsOn: []string{"C"}})
	dag.AddNode(&Node{Name: "C"})
	dag.AddNode(&Node{Name: "D"}) // independent

	sub := dag.Select([]string{"A"})

	if len(sub.Nodes) != 3 {
		t.Fatalf("subgraph has %d nodes, want 3 (A, B, C)", len(sub.Nodes))
	}
	for _, name := range []string{"A", "B", "C"} {
		if _, ok := sub.Nodes[name]; !ok {
			t.Errorf("subgraph missing node %s", name)
		}
	}
	if _, ok := sub.Nodes["D"]; ok {
		t.Error("subgraph should not include D")
	}
}

func TestSelectSubgraphPartial(t *testing.T) {
	dag := NewDAG()
	dag.AddNode(&Node{Name: "A", DependsOn: []string{"B"}})
	dag.AddNode(&Node{Name: "B"})
	dag.AddNode(&Node{Name: "C"})

	sub := dag.Select([]string{"B"})
	if len(sub.Nodes) != 1 {
		t.Fatalf("subgraph has %d nodes, want 1", len(sub.Nodes))
	}
	if _, ok := sub.Nodes["B"]; !ok {
		t.Error("subgraph missing node B")
	}
}

func TestEmptyDAG(t *testing.T) {
	dag := NewDAG()
	sorted, err := dag.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 0 {
		t.Errorf("got %d nodes, want 0", len(sorted))
	}
}

func TestSelectEmpty(t *testing.T) {
	dag := NewDAG()
	dag.AddNode(&Node{Name: "A"})

	sub := dag.Select([]string{})
	if len(sub.Nodes) != 0 {
		t.Errorf("expected empty subgraph, got %d nodes", len(sub.Nodes))
	}
}

func TestSelectNonExistentNode(t *testing.T) {
	dag := NewDAG()
	dag.AddNode(&Node{Name: "A"})

	sub := dag.Select([]string{"Z"})
	if len(sub.Nodes) != 0 {
		t.Errorf("expected empty subgraph for non-existent node, got %d nodes", len(sub.Nodes))
	}
}
