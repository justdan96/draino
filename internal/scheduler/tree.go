package scheduler

import (
	url2 "net/url"
	"sort"
	"strings"
)

type SortingTree[T any] interface {
	Initialize(elements []T, sorters []LessFunc[T])
	Next() (T, bool)
	AsDotGraph(url bool) string
}

type LessFunc[T any] func(i, j T) bool

func bucketSlice[T any](s []T, less LessFunc[T]) [][]T {
	if len(s) == 0 {
		return nil
	}
	sort.Slice(s, func(i, j int) bool { return less(s[i], s[j]) })
	current := []T{s[0]}
	var buckets [][]T
	for i := 1; i < len(s); i++ {
		if !less(s[i-1], s[i]) && !less(s[i], s[i-1]) { // element are equal a go to same bucket
			current = append(current, s[i])
			continue
		}
		buckets = append(buckets, current)
		current = []T{s[i]}
	}
	buckets = append(buckets, current)
	return buckets
}

// Validate that the SortingTreeImpl correctly implement the interface
var _ SortingTree[any] = &sortingTreeImpl[any]{}

type sortingTreeImpl[T any] struct {
	root        *node[T]
	sorters     []LessFunc[T]
	currentNode *node[T]
}

func NewSortingTree[T any](elements []T, sorters []LessFunc[T]) SortingTree[T] {
	t := &sortingTreeImpl[T]{}
	t.Initialize(elements, sorters)
	return t
}

func (s *sortingTreeImpl[T]) Initialize(elements []T, sorters []LessFunc[T]) {
	s.root = &node[T]{
		rawCollection: elements,
		sorters:       sorters,
		parent:        nil,
		children:      nil,
		current:       0,
	}
	s.currentNode = s.root
	s.sorters = sorters
}

func (s *sortingTreeImpl[T]) Next() (T, bool) {
	var null T
	// if no current node that means that we are done with exploration of the tree
	if s.currentNode == nil {
		return null, false
	}
	// check if current node is expended
	t, n, found := s.currentNode.next()
	s.currentNode = n
	if found {
		return t, true
	}
	return null, false
}

type node[T any] struct {
	rawCollection []T
	sorters       []LessFunc[T]

	parent   *node[T]
	children []*node[T]
	current  int
}

func (n *node[T]) expand() {
	if n.children != nil {
		return // already expanded
	}
	if n.sorters != nil && len(n.sorters) > 0 {
		buckets := bucketSlice(n.rawCollection, n.sorters[0])

		for _, b := range buckets {
			newNode := &node[T]{rawCollection: b, sorters: n.sorters[1:], parent: n}
			n.children = append(n.children, newNode)
		}
	}
}

func (n *node[T]) isLeaf() bool {
	if n.sorters == nil || len(n.sorters) == 0 || len(n.rawCollection) == 1 {
		return true
	}
	return false
}

func (n *node[T]) next() (T, *node[T], bool) {
	var null T
	if n.isLeaf() {
		if n.current < len(n.rawCollection) {
			n.current++
			return n.rawCollection[n.current-1], n, true
		}

		if n.parent == nil {
			return null, nil, false
		}
		n.parent.current++
		return n.parent.next()
	}

	n.expand()
	if n.current < len(n.children) {
		return n.children[n.current].next()
	}

	if n.parent == nil {
		return null, nil, false
	}
	n.parent.current++
	return n.parent.next()
}

func (s *sortingTreeImpl[T]) AsDotGraph(url bool) string {
	g := newGraph[T]()
	if s.currentNode != nil {
		g.setCurrent(s.currentNode)
	}
	s.root.buildDotGraph(g, "R")
	if !url {
		return g.String()
	}
	t := &url2.URL{Path: g.String()}
	return "https://dreampuf.github.io/GraphvizOnline/#" + strings.TrimLeft(t.String(), "./")

	//digraph%20G%20%7B%0A%0A%20%20subgraph%20cluster_0%20%7B%0A%20%20%20%20style%3Dfilled%3B%0A%20%20%20%20color%3Dlightgrey%3B%0A%20%20%20%20node%20%5Bstyle%3Dfilled%2Ccolor%3Dwhite%5D%3B%0A%20%20%20%20a0%20-%3E%20a1%20-%3E%20a2%20-%3E%20a3%3B%0A%20%20%20%20label%20%3D%20%22process%20%231%22%3B%0A%20%20%7D%0A%0A%20%20subgraph%20cluster_1%20%7B%0A%20%20%20%20node%20%5Bstyle%3Dfilled%5D%3B%0A%20%20%20%20b0%20-%3E%20b1%20-%3E%20b2%20-%3E%20b3%3B%0A%20%20%20%20label%20%3D%20%22process%20%232%22%3B%0A%20%20%20%20color%3Dblue%0A%20%20%7D%0A%20%20start%20-%3E%20a0%3B%0A%20%20start%20-%3E%20b0%3B%0A%20%20a1%20-%3E%20b3%3B%0A%20%20b2%20-%3E%20a3%3B%0A%20%20a3%20-%3E%20a0%3B%0A%20%20a3%20-%3E%20end%3B%0A%20%20b3%20-%3E%20end%3B%0A%0A%20%20start%20%5Bshape%3DMdiamond%5D%3B%0A%20%20end%20%5Bshape%3DMsquare%5D%3B%0A%7D
}
