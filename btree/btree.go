package btree

import (
	"cmp"
	"math"
)

// BTree is a B-tree with ordered keys and any values.
type BTree[K cmp.Ordered] struct {
	root   *node[K]
	degree int // minimum degree (t): each node has at most 2t-1 keys
	length int
}

type entry[K cmp.Ordered] struct {
	key   K
	value any
}

type node[K cmp.Ordered] struct {
	entries  []entry[K]
	children []*node[K]
	leaf     bool
}

// New creates a new BTree with the given degree (minimum degree t).
// Each node can hold at most 2*degree-1 keys.
func New[K cmp.Ordered](degree int) *BTree[K] {
	if degree < 2 {
		degree = 2
	}
	return &BTree[K]{
		root:   &node[K]{leaf: true},
		degree: degree,
	}
}

// Len returns the number of entries in the tree.
func (t *BTree[K]) Len() int {
	return t.length
}

// Has returns true if the key exists in the tree.
func (t *BTree[K]) Has(key K) bool {
	_, ok := t.Get(key)
	return ok
}

// Get retrieves the value for the given key.
func (t *BTree[K]) Get(key K) (any, bool) {
	return t.root.get(key)
}

func (n *node[K]) get(key K) (any, bool) {
	i := n.search(key)
	if i < len(n.entries) && n.entries[i].key == key {
		return n.entries[i].value, true
	}
	if n.leaf {
		return nil, false
	}
	return n.children[i].get(key)
}

// search returns the index of the first entry with key >= the given key.
func (n *node[K]) search(key K) int {
	lo, hi := 0, len(n.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if n.entries[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Insert inserts a key-value pair. Returns false if the key already exists.
func (t *BTree[K]) Insert(key K, value any) bool {
	if t.root.has(key) {
		return false
	}
	t.insert(key, value)
	return true
}

func (n *node[K]) has(key K) bool {
	i := n.search(key)
	if i < len(n.entries) && n.entries[i].key == key {
		return true
	}
	if n.leaf {
		return false
	}
	return n.children[i].has(key)
}

// Put inserts or updates a key-value pair (upsert).
func (t *BTree[K]) Put(key K, value any) {
	if t.root.put(key, value) {
		return
	}
	t.insert(key, value)
}

func (n *node[K]) put(key K, value any) bool {
	i := n.search(key)
	if i < len(n.entries) && n.entries[i].key == key {
		n.entries[i].value = value
		return true
	}
	if n.leaf {
		return false
	}
	return n.children[i].put(key, value)
}

func (t *BTree[K]) insert(key K, value any) {
	root := t.root
	if len(root.entries) == 2*t.degree-1 {
		// Root is full, split it
		newRoot := &node[K]{leaf: false}
		newRoot.children = append(newRoot.children, root)
		t.splitChild(newRoot, 0)
		t.root = newRoot
	}
	t.insertNonFull(t.root, entry[K]{key: key, value: value})
	t.length++
}

func (t *BTree[K]) insertNonFull(n *node[K], e entry[K]) {
	i := n.search(e.key)
	if n.leaf {
		// Insert entry at position i
		n.entries = append(n.entries, entry[K]{})
		copy(n.entries[i+1:], n.entries[i:])
		n.entries[i] = e
		return
	}
	if len(n.children[i].entries) == 2*t.degree-1 {
		t.splitChild(n, i)
		if e.key > n.entries[i].key {
			i++
		}
	}
	t.insertNonFull(n.children[i], e)
}

func (t *BTree[K]) splitChild(parent *node[K], i int) {
	deg := t.degree
	child := parent.children[i]
	mid := deg - 1

	// New node gets the right half
	sibling := &node[K]{leaf: child.leaf}
	sibling.entries = make([]entry[K], len(child.entries[mid+1:]))
	copy(sibling.entries, child.entries[mid+1:])

	if !child.leaf {
		sibling.children = make([]*node[K], len(child.children[mid+1:]))
		copy(sibling.children, child.children[mid+1:])
	}

	// Promote middle entry to parent
	medianEntry := child.entries[mid]

	// Truncate child
	child.entries = child.entries[:mid]
	if !child.leaf {
		child.children = child.children[:mid+1]
	}

	// Insert sibling into parent's children
	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = sibling

	// Insert median into parent's entries
	parent.entries = append(parent.entries, entry[K]{})
	copy(parent.entries[i+1:], parent.entries[i:])
	parent.entries[i] = medianEntry
}

// Delete removes a key from the tree. Returns false if the key was not found.
func (t *BTree[K]) Delete(key K) bool {
	if t.root == nil {
		return false
	}
	deleted := t.delete(t.root, key)
	if deleted {
		t.length--
		// If root has no entries and has a child, shrink tree
		if len(t.root.entries) == 0 && !t.root.leaf {
			t.root = t.root.children[0]
		}
	}
	return deleted
}

func (t *BTree[K]) delete(n *node[K], key K) bool {
	i := n.search(key)

	if i < len(n.entries) && n.entries[i].key == key {
		// Key found in this node
		if n.leaf {
			// Case 1: Key is in a leaf node
			n.entries = append(n.entries[:i], n.entries[i+1:]...)
			return true
		}
		// Case 2: Key is in an internal node
		return t.deleteInternal(n, i)
	}

	if n.leaf {
		return false // Key not found
	}

	// Key might be in child[i]
	return t.deleteFromChild(n, i, key)
}

func (t *BTree[K]) deleteInternal(n *node[K], i int) bool {
	key := n.entries[i].key
	// Case 2a: Left child has enough entries
	if len(n.children[i].entries) >= t.degree {
		pred := t.predecessor(n.children[i])
		n.entries[i] = pred
		return t.delete(n.children[i], pred.key)
	}
	// Case 2b: Right child has enough entries
	if len(n.children[i+1].entries) >= t.degree {
		succ := t.successor(n.children[i+1])
		n.entries[i] = succ
		return t.delete(n.children[i+1], succ.key)
	}
	// Case 2c: Both children have t-1 entries, merge
	t.merge(n, i)
	return t.delete(n.children[i], key)
}

func (t *BTree[K]) deleteFromChild(n *node[K], i int, key K) bool {
	child := n.children[i]
	if len(child.entries) < t.degree {
		t.fill(n, i)
		// After fill, structure may have changed (merge/borrow).
		// Re-do delete from this node to find correct path.
		return t.delete(n, key)
	}
	return t.delete(child, key)
}

func (t *BTree[K]) predecessor(n *node[K]) entry[K] {
	for !n.leaf {
		n = n.children[len(n.children)-1]
	}
	return n.entries[len(n.entries)-1]
}

func (t *BTree[K]) successor(n *node[K]) entry[K] {
	for !n.leaf {
		n = n.children[0]
	}
	return n.entries[0]
}

func (t *BTree[K]) fill(parent *node[K], i int) {
	if i > 0 && len(parent.children[i-1].entries) >= t.degree {
		t.borrowFromLeft(parent, i)
	} else if i < len(parent.children)-1 && len(parent.children[i+1].entries) >= t.degree {
		t.borrowFromRight(parent, i)
	} else {
		if i < len(parent.children)-1 {
			t.merge(parent, i)
		} else {
			t.merge(parent, i-1)
		}
	}
}

func (t *BTree[K]) borrowFromLeft(parent *node[K], i int) {
	child := parent.children[i]
	left := parent.children[i-1]

	// Shift child entries/children right
	child.entries = append([]entry[K]{parent.entries[i-1]}, child.entries...)
	if !child.leaf {
		child.children = append([]*node[K]{left.children[len(left.children)-1]}, child.children...)
		left.children = left.children[:len(left.children)-1]
	}

	parent.entries[i-1] = left.entries[len(left.entries)-1]
	left.entries = left.entries[:len(left.entries)-1]
}

func (t *BTree[K]) borrowFromRight(parent *node[K], i int) {
	child := parent.children[i]
	right := parent.children[i+1]

	child.entries = append(child.entries, parent.entries[i])
	if !child.leaf {
		child.children = append(child.children, right.children[0])
		right.children = right.children[1:]
	}

	parent.entries[i] = right.entries[0]
	right.entries = right.entries[1:]
}

func (t *BTree[K]) merge(parent *node[K], i int) {
	left := parent.children[i]
	right := parent.children[i+1]

	// Merge: left + parent[i] + right
	left.entries = append(left.entries, parent.entries[i])
	left.entries = append(left.entries, right.entries...)
	if !left.leaf {
		left.children = append(left.children, right.children...)
	}

	// Remove parent entry and right child
	parent.entries = append(parent.entries[:i], parent.entries[i+1:]...)
	parent.children = append(parent.children[:i+1], parent.children[i+2:]...)
}

// ForEach iterates over all entries in key order.
// The callback should return true to continue, false to stop.
func (t *BTree[K]) ForEach(fn func(key K, value any) bool) {
	if t.root != nil {
		t.root.forEach(fn)
	}
}

// ForEachRange iterates over entries whose keys fall within the specified range.
// from == nil means no lower bound; to == nil means no upper bound.
// fromInclusive/toInclusive control whether the boundaries are included.
// The callback should return true to continue, false to stop.
func (t *BTree[K]) ForEachRange(from *K, fromInclusive bool, to *K, toInclusive bool, fn func(key K, value any) bool) {
	if t.root != nil {
		t.root.forEachRange(from, fromInclusive, to, toInclusive, fn)
	}
}

func (n *node[K]) forEachRange(from *K, fromInclusive bool, to *K, toInclusive bool, fn func(key K, value any) bool) bool {
	// Find the starting index: first entry with key >= from (or > from if not inclusive)
	startIdx := 0
	if from != nil {
		startIdx = n.search(*from)
	}

	for i := startIdx; i < len(n.entries); i++ {
		// Visit left child first (if not leaf)
		if !n.leaf {
			if !n.children[i].forEachRange(from, fromInclusive, to, toInclusive, fn) {
				return false
			}
		}

		key := n.entries[i].key

		// Check lower bound
		if from != nil {
			if fromInclusive {
				if key < *from {
					continue
				}
			} else {
				if key <= *from {
					continue
				}
			}
		}

		// Check upper bound
		if to != nil {
			if toInclusive {
				if key > *to {
					return false
				}
			} else {
				if key >= *to {
					return false
				}
			}
		}

		if !fn(key, n.entries[i].value) {
			return false
		}
	}

	// Visit the last child
	if !n.leaf {
		return n.children[len(n.entries)].forEachRange(from, fromInclusive, to, toInclusive, fn)
	}
	return true
}

// ForEachReverse iterates over all entries in descending key order.
// The callback should return true to continue, false to stop.
func (t *BTree[K]) ForEachReverse(fn func(key K, value any) bool) {
	if t.root != nil {
		t.root.forEachReverse(fn)
	}
}

func (n *node[K]) forEachReverse(fn func(key K, value any) bool) bool {
	// Visit rightmost child first, then entries right-to-left
	if !n.leaf {
		if !n.children[len(n.entries)].forEachReverse(fn) {
			return false
		}
	}
	for i := len(n.entries) - 1; i >= 0; i-- {
		if !fn(n.entries[i].key, n.entries[i].value) {
			return false
		}
		if !n.leaf {
			if !n.children[i].forEachReverse(fn) {
				return false
			}
		}
	}
	return true
}

// ForEachRangeReverse iterates over entries whose keys fall within the specified range in descending order.
// from == nil means no lower bound; to == nil means no upper bound.
// fromInclusive/toInclusive control whether the boundaries are included.
// The callback should return true to continue, false to stop.
func (t *BTree[K]) ForEachRangeReverse(from *K, fromInclusive bool, to *K, toInclusive bool, fn func(key K, value any) bool) {
	if t.root != nil {
		t.root.forEachRangeReverse(from, fromInclusive, to, toInclusive, fn)
	}
}

func (n *node[K]) forEachRangeReverse(from *K, fromInclusive bool, to *K, toInclusive bool, fn func(key K, value any) bool) bool {
	// Find starting index: we scan entries from right to left
	endIdx := len(n.entries) - 1
	if to != nil {
		// Find position of *to; entries[i] with key > *to can be skipped
		endIdx = n.search(*to)
		if endIdx < len(n.entries) && n.entries[endIdx].key == *to {
			if !toInclusive {
				endIdx--
			}
		} else {
			endIdx--
		}
	}

	// Visit the rightmost child that could contain keys <= endIdx entry
	if !n.leaf && endIdx+1 < len(n.children) {
		if !n.children[endIdx+1].forEachRangeReverse(from, fromInclusive, to, toInclusive, fn) {
			return false
		}
	}

	for i := endIdx; i >= 0; i-- {
		key := n.entries[i].key

		// Check lower bound: if key is below the lower bound, stop
		if from != nil {
			if fromInclusive {
				if key < *from {
					return true
				}
			} else {
				if key <= *from {
					return true
				}
			}
		}

		// Check upper bound: skip keys above upper bound
		if to != nil {
			if toInclusive {
				if key > *to {
					goto visitChild
				}
			} else {
				if key >= *to {
					goto visitChild
				}
			}
		}

		if !fn(key, n.entries[i].value) {
			return false
		}

	visitChild:
		if !n.leaf {
			if !n.children[i].forEachRangeReverse(from, fromInclusive, to, toInclusive, fn) {
				return false
			}
		}
	}

	return true
}

func (n *node[K]) forEach(fn func(key K, value any) bool) bool {
	for i, e := range n.entries {
		if !n.leaf {
			if !n.children[i].forEach(fn) {
				return false
			}
		}
		if !fn(e.key, e.value) {
			return false
		}
	}
	if !n.leaf {
		return n.children[len(n.entries)].forEach(fn)
	}
	return true
}

// --- Serialization support ---

// EntryData is an exported key-value pair for serialization.
type EntryData[K cmp.Ordered] struct {
	Key   K
	Value any
}

// NodeData is an exported node representation for serialization.
type NodeData[K cmp.Ordered] struct {
	Leaf     bool
	Entries  []EntryData[K]
	Children []uint32 // child page IDs (nil for leaf nodes)
}

// Degree returns the minimum degree (t) of the tree.
func (t *BTree[K]) Degree() int {
	return t.degree
}

// WalkNodes performs a post-order traversal of the tree, calling fn for each node.
// fn receives the node data and returns a page ID to assign to that node.
// Returns the root page ID and whether the tree has a root (is non-empty).
func (t *BTree[K]) WalkNodes(fn func(data NodeData[K]) uint32) (rootPageID uint32, hasRoot bool) {
	if t.root == nil || (t.root.leaf && len(t.root.entries) == 0) {
		return math.MaxUint32, false
	}
	id := walkNode(t.root, fn)
	return id, true
}

func walkNode[K cmp.Ordered](n *node[K], fn func(data NodeData[K]) uint32) uint32 {
	data := NodeData[K]{
		Leaf: n.leaf,
	}

	// Convert entries
	data.Entries = make([]EntryData[K], len(n.entries))
	for i, e := range n.entries {
		data.Entries[i] = EntryData[K]{Key: e.key, Value: e.value}
	}

	// Process children first (post-order) and collect page IDs
	if !n.leaf {
		data.Children = make([]uint32, len(n.children))
		for i, child := range n.children {
			data.Children[i] = walkNode(child, fn)
		}
	}

	return fn(data)
}

// BuildFromNodes reconstructs a BTree from serialized node data.
// loadNode is called to load the node data for a given page ID.
func BuildFromNodes[K cmp.Ordered](degree int, length int, rootPageID uint32, loadNode func(pageID uint32) NodeData[K]) *BTree[K] {
	if degree < 2 {
		degree = 2
	}
	t := &BTree[K]{
		degree: degree,
		length: length,
	}
	t.root = buildNode(rootPageID, loadNode)
	return t
}

func buildNode[K cmp.Ordered](pageID uint32, loadNode func(pageID uint32) NodeData[K]) *node[K] {
	data := loadNode(pageID)
	n := &node[K]{
		leaf: data.Leaf,
	}

	// Convert entries
	n.entries = make([]entry[K], len(data.Entries))
	for i, e := range data.Entries {
		n.entries[i] = entry[K]{key: e.Key, value: e.Value}
	}

	// Build children
	if !data.Leaf && len(data.Children) > 0 {
		n.children = make([]*node[K], len(data.Children))
		for i, childID := range data.Children {
			n.children[i] = buildNode(childID, loadNode)
		}
	}

	return n
}
