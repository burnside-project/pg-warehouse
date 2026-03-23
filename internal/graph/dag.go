package graph

import "fmt"

// Node represents a model in the dependency graph.
type Node struct {
	Name      string
	DependsOn []string
	Layer     string
}

// DAG represents a directed acyclic graph of model dependencies.
type DAG struct {
	Nodes map[string]*Node
}

// NewDAG creates an empty DAG.
func NewDAG() *DAG {
	return &DAG{Nodes: make(map[string]*Node)}
}

// AddNode adds a model node to the graph.
func (d *DAG) AddNode(node *Node) {
	d.Nodes[node.Name] = node
}

// TopologicalSort returns nodes in execution order using Kahn's algorithm.
// Returns error if a cycle is detected.
func (d *DAG) TopologicalSort() ([]*Node, error) {
	// Build in-degree map.
	// If node A depends on B, there is an edge B -> A, so in-degree[A]++.
	inDegree := make(map[string]int)
	for name := range d.Nodes {
		inDegree[name] = 0
	}
	for _, node := range d.Nodes {
		for _, dep := range node.DependsOn {
			if _, exists := d.Nodes[dep]; exists {
				inDegree[node.Name]++
			}
		}
	}

	// Seed queue with nodes that have no dependencies.
	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var sorted []*Node
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		sorted = append(sorted, d.Nodes[current])

		// For each node that depends on current, decrement its in-degree.
		for _, node := range d.Nodes {
			for _, dep := range node.DependsOn {
				if dep == current {
					inDegree[node.Name]--
					if inDegree[node.Name] == 0 {
						queue = append(queue, node.Name)
					}
				}
			}
		}
	}

	if len(sorted) != len(d.Nodes) {
		return nil, fmt.Errorf("cycle detected: sorted %d of %d nodes", len(sorted), len(d.Nodes))
	}

	return sorted, nil
}

// Select returns a subgraph containing only the named nodes and their
// transitive dependencies.
func (d *DAG) Select(names []string) *DAG {
	selected := make(map[string]bool)
	var walk func(name string)
	walk = func(name string) {
		if selected[name] {
			return
		}
		if node, exists := d.Nodes[name]; exists {
			selected[name] = true
			for _, dep := range node.DependsOn {
				walk(dep)
			}
		}
	}
	for _, name := range names {
		walk(name)
	}

	sub := NewDAG()
	for name := range selected {
		if node, exists := d.Nodes[name]; exists {
			sub.AddNode(node)
		}
	}
	return sub
}
