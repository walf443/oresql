package btree

import (
	"cmp"
	"math"
)

// BTree is a B+Tree with ordered keys and any values.
// All data is stored in leaf nodes only. Internal nodes hold routing keys.
// Leaf nodes are linked via next pointers for efficient range scans.
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
	next     *node[K] // next leaf pointer (only used for leaf nodes)
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

// LeftmostLeaf returns the leftmost leaf node (for testing/traversal).
func (t *BTree[K]) LeftmostLeaf() *node[K] {
	if t.root == nil {
		return nil
	}
	n := t.root
	for !n.leaf {
		n = n.children[0]
	}
	return n
}

// Get retrieves the value for the given key.
// In B+Tree, values are only stored in leaf nodes.
func (t *BTree[K]) Get(key K) (any, bool) {
	n := t.root
	for !n.leaf {
		i := n.findChild(key)
		n = n.children[i]
	}
	// Search in leaf
	i := n.search(key)
	if i < len(n.entries) && n.entries[i].key == key {
		return n.entries[i].value, true
	}
	return nil, false
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

// findChild returns the child index to follow for the given key.
// Uses upper-bound binary search: finds the first entry with key > given key.
// entries = [5, 10, 15], children = [c0, c1, c2, c3]
// findChild(3)=0, findChild(5)=1, findChild(10)=2, findChild(20)=3
func (n *node[K]) findChild(key K) int {
	lo, hi := 0, len(n.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if n.entries[mid].key <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Insert inserts a key-value pair. Returns false if the key already exists.
func (t *BTree[K]) Insert(key K, value any) bool {
	if t.Has(key) {
		return false
	}
	t.insert(key, value)
	return true
}

// Put inserts or updates a key-value pair (upsert).
func (t *BTree[K]) Put(key K, value any) {
	// Try to find and update in leaf
	n := t.root
	for !n.leaf {
		i := n.findChild(key)
		n = n.children[i]
	}
	i := n.search(key)
	if i < len(n.entries) && n.entries[i].key == key {
		n.entries[i].value = value
		return
	}
	t.insert(key, value)
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
	if n.leaf {
		// Insert entry at sorted position
		i := n.search(e.key)
		n.entries = append(n.entries, entry[K]{})
		copy(n.entries[i+1:], n.entries[i:])
		n.entries[i] = e
		return
	}
	// Find child to descend into
	i := n.findChild(e.key)
	if len(n.children[i].entries) == 2*t.degree-1 {
		t.splitChild(n, i)
		// After split, n.entries[i] is the separator key.
		// Decide which child to descend into.
		if e.key >= n.entries[i].key {
			i++
		}
	}
	t.insertNonFull(n.children[i], e)
}

// splitChild splits the child at index i of parent.
// For leaf splits: the separator key is copied to the parent (data stays in leaves).
// For internal splits: the separator key is moved to the parent (B-Tree style).
func (t *BTree[K]) splitChild(parent *node[K], i int) {
	deg := t.degree
	child := parent.children[i]
	mid := deg - 1

	sibling := &node[K]{leaf: child.leaf}

	if child.leaf {
		// Leaf split: copy all entries from mid onward to sibling
		sibling.entries = make([]entry[K], len(child.entries[mid:]))
		copy(sibling.entries, child.entries[mid:])

		// Truncate child to [0..mid)
		child.entries = child.entries[:mid]

		// Link leaf chain
		sibling.next = child.next
		child.next = sibling

		// Separator key is the first key of the sibling (copy up)
		separatorEntry := entry[K]{key: sibling.entries[0].key, value: nil}

		// Insert sibling into parent's children
		parent.children = append(parent.children, nil)
		copy(parent.children[i+2:], parent.children[i+1:])
		parent.children[i+1] = sibling

		// Insert separator into parent's entries
		parent.entries = append(parent.entries, entry[K]{})
		copy(parent.entries[i+1:], parent.entries[i:])
		parent.entries[i] = separatorEntry
	} else {
		// Internal split: move the median key up (B-Tree style)
		sibling.entries = make([]entry[K], len(child.entries[mid+1:]))
		copy(sibling.entries, child.entries[mid+1:])

		sibling.children = make([]*node[K], len(child.children[mid+1:]))
		copy(sibling.children, child.children[mid+1:])

		// The median entry becomes the separator (moved up, value is nil for internal)
		separatorEntry := entry[K]{key: child.entries[mid].key, value: nil}

		// Truncate child
		child.entries = child.entries[:mid]
		child.children = child.children[:mid+1]

		// Insert sibling into parent's children
		parent.children = append(parent.children, nil)
		copy(parent.children[i+2:], parent.children[i+1:])
		parent.children[i+1] = sibling

		// Insert separator into parent's entries
		parent.entries = append(parent.entries, entry[K]{})
		copy(parent.entries[i+1:], parent.entries[i:])
		parent.entries[i] = separatorEntry
	}
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

// delete removes a key from the subtree rooted at n.
// In B+Tree, values only exist in leaves, so we always descend to a leaf.
func (t *BTree[K]) delete(n *node[K], key K) bool {
	if n.leaf {
		// Search for the key in this leaf
		i := n.search(key)
		if i < len(n.entries) || (i < len(n.entries) && n.entries[i].key == key) {
			// Check again properly
		}
		i = n.search(key)
		if i >= len(n.entries) || n.entries[i].key != key {
			return false // Key not found
		}
		// Remove entry from leaf
		n.entries = append(n.entries[:i], n.entries[i+1:]...)
		return true
	}

	// Internal node: find which child to descend into
	i := n.findChild(key)

	// Ensure child has enough entries before descending
	if len(n.children[i].entries) < t.degree {
		t.fill(n, i)
		// After fill, structure may have changed. Re-find the child.
		i = n.findChild(key)
		if i >= len(n.children) {
			i = len(n.children) - 1
		}
	}

	deleted := t.delete(n.children[i], key)

	// After deletion from child, update separator keys if needed
	if deleted {
		t.updateKeys(n, key)
	}

	return deleted
}

// updateKeys updates internal node separator keys after a deletion.
// If the deleted key was used as a separator, replace it with the new minimum
// key of the corresponding subtree.
func (t *BTree[K]) updateKeys(n *node[K], deletedKey K) {
	if n.leaf {
		return
	}
	for i := 0; i < len(n.entries); i++ {
		if n.entries[i].key == deletedKey {
			// Find the minimum key in children[i+1]
			minKey := t.findMinKey(n.children[i+1])
			n.entries[i].key = minKey
		}
	}
}

// findMinKey returns the minimum key in the subtree rooted at n.
func (t *BTree[K]) findMinKey(n *node[K]) K {
	for !n.leaf {
		n = n.children[0]
	}
	return n.entries[0].key
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

	if child.leaf {
		// Leaf: move last entry from left sibling to front of child
		movedEntry := left.entries[len(left.entries)-1]
		child.entries = append([]entry[K]{movedEntry}, child.entries...)
		left.entries = left.entries[:len(left.entries)-1]

		// Update separator in parent to the new first key of child
		parent.entries[i-1].key = child.entries[0].key
	} else {
		// Internal: rotate through parent
		child.entries = append([]entry[K]{parent.entries[i-1]}, child.entries...)
		child.children = append([]*node[K]{left.children[len(left.children)-1]}, child.children...)
		parent.entries[i-1] = left.entries[len(left.entries)-1]
		left.entries = left.entries[:len(left.entries)-1]
		left.children = left.children[:len(left.children)-1]
	}
}

func (t *BTree[K]) borrowFromRight(parent *node[K], i int) {
	child := parent.children[i]
	right := parent.children[i+1]

	if child.leaf {
		// Leaf: move first entry from right sibling to end of child
		movedEntry := right.entries[0]
		child.entries = append(child.entries, movedEntry)
		right.entries = right.entries[1:]

		// Update separator in parent to the new first key of right
		parent.entries[i].key = right.entries[0].key
	} else {
		// Internal: rotate through parent
		child.entries = append(child.entries, parent.entries[i])
		child.children = append(child.children, right.children[0])
		parent.entries[i] = right.entries[0]
		right.entries = right.entries[1:]
		right.children = right.children[1:]
	}
}

func (t *BTree[K]) merge(parent *node[K], i int) {
	left := parent.children[i]
	right := parent.children[i+1]

	if left.leaf {
		// Leaf merge: just concatenate entries, update next pointer
		left.entries = append(left.entries, right.entries...)
		left.next = right.next
	} else {
		// Internal merge: bring down separator from parent
		left.entries = append(left.entries, parent.entries[i])
		left.entries = append(left.entries, right.entries...)
		left.children = append(left.children, right.children...)
	}

	// Remove parent entry and right child
	parent.entries = append(parent.entries[:i], parent.entries[i+1:]...)
	parent.children = append(parent.children[:i+1], parent.children[i+2:]...)
}

// ForEach iterates over all entries in key order.
// In B+Tree, we simply follow the leaf chain.
// The callback should return true to continue, false to stop.
func (t *BTree[K]) ForEach(fn func(key K, value any) bool) {
	if t.root == nil {
		return
	}
	// Find leftmost leaf
	n := t.root
	for !n.leaf {
		n = n.children[0]
	}
	// Traverse leaf chain
	for n != nil {
		for _, e := range n.entries {
			if !fn(e.key, e.value) {
				return
			}
		}
		n = n.next
	}
}

// ForEachRange iterates over entries whose keys fall within the specified range.
// from == nil means no lower bound; to == nil means no upper bound.
// fromInclusive/toInclusive control whether the boundaries are included.
// The callback should return true to continue, false to stop.
func (t *BTree[K]) ForEachRange(from *K, fromInclusive bool, to *K, toInclusive bool, fn func(key K, value any) bool) {
	if t.root == nil {
		return
	}

	// Find the starting leaf
	var startLeaf *node[K]
	if from == nil {
		// No lower bound: start from leftmost leaf
		n := t.root
		for !n.leaf {
			n = n.children[0]
		}
		startLeaf = n
	} else {
		// Find the leaf that may contain *from
		startLeaf = t.findLeaf(*from)
	}

	// Traverse leaf chain from startLeaf
	for n := startLeaf; n != nil; n = n.next {
		for _, e := range n.entries {
			// Check lower bound
			if from != nil {
				if fromInclusive {
					if e.key < *from {
						continue
					}
				} else {
					if e.key <= *from {
						continue
					}
				}
			}

			// Check upper bound
			if to != nil {
				if toInclusive {
					if e.key > *to {
						return
					}
				} else {
					if e.key >= *to {
						return
					}
				}
			}

			if !fn(e.key, e.value) {
				return
			}
		}
	}
}

// findLeaf finds the leaf node that would contain the given key.
func (t *BTree[K]) findLeaf(key K) *node[K] {
	n := t.root
	for !n.leaf {
		i := n.findChild(key)
		n = n.children[i]
	}
	return n
}

// ForEachReverse iterates over all entries in descending key order.
// The callback should return true to continue, false to stop.
func (t *BTree[K]) ForEachReverse(fn func(key K, value any) bool) {
	if t.root != nil {
		t.root.forEachReverse(fn)
	}
}

func (n *node[K]) forEachReverse(fn func(key K, value any) bool) bool {
	if n.leaf {
		// Leaf: iterate entries in reverse
		for i := len(n.entries) - 1; i >= 0; i-- {
			if !fn(n.entries[i].key, n.entries[i].value) {
				return false
			}
		}
		return true
	}
	// Internal: visit rightmost child first, then entries right-to-left
	if !n.children[len(n.entries)].forEachReverse(fn) {
		return false
	}
	for i := len(n.entries) - 1; i >= 0; i-- {
		if !n.children[i].forEachReverse(fn) {
			return false
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
	if n.leaf {
		// Leaf: iterate entries in reverse, applying range filters
		for i := len(n.entries) - 1; i >= 0; i-- {
			key := n.entries[i].key

			// Check upper bound: skip keys above upper bound
			if to != nil {
				if toInclusive {
					if key > *to {
						continue
					}
				} else {
					if key >= *to {
						continue
					}
				}
			}

			// Check lower bound: stop if below lower bound
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

			if !fn(key, n.entries[i].value) {
				return false
			}
		}
		return true
	}

	// Internal node: find starting position
	endIdx := len(n.entries)
	if to != nil {
		// Find the child index for the upper bound
		endIdx = n.findChild(*to)
	}

	// Visit the rightmost relevant child first
	if endIdx < len(n.children) {
		if !n.children[endIdx].forEachRangeReverse(from, fromInclusive, to, toInclusive, fn) {
			return false
		}
	}

	// Visit remaining children right-to-left
	for i := endIdx - 1; i >= 0; i-- {
		// Check if we need to continue (lower bound check on separator)
		if from != nil {
			if fromInclusive {
				if n.entries[i].key < *from {
					return true
				}
			} else {
				if n.entries[i].key <= *from {
					// Still need to visit children[i] since it might contain keys > from
					if !n.children[i].forEachRangeReverse(from, fromInclusive, to, toInclusive, fn) {
						return false
					}
					return true
				}
			}
		}
		if !n.children[i].forEachRangeReverse(from, fromInclusive, to, toInclusive, fn) {
			return false
		}
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
	rebuildLeafChain(t.root)
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

// rebuildLeafChain rebuilds the next pointers for all leaf nodes
// by performing an in-order traversal and linking leaves.
func rebuildLeafChain[K cmp.Ordered](root *node[K]) {
	if root == nil {
		return
	}
	var prev *node[K]
	var linkLeaves func(n *node[K])
	linkLeaves = func(n *node[K]) {
		if n.leaf {
			if prev != nil {
				prev.next = n
			}
			prev = n
			return
		}
		for _, child := range n.children {
			linkLeaves(child)
		}
	}
	linkLeaves(root)
}
