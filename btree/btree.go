package btree

// BTree is a B-tree with int64 keys and any values.
type BTree struct {
	root   *node
	degree int // minimum degree (t): each node has at most 2t-1 keys
	length int
}

type entry struct {
	key   int64
	value any
}

type node struct {
	entries  []entry
	children []*node
	leaf     bool
}

// New creates a new BTree with the given degree (minimum degree t).
// Each node can hold at most 2*degree-1 keys.
func New(degree int) *BTree {
	if degree < 2 {
		degree = 2
	}
	return &BTree{
		root:   &node{leaf: true},
		degree: degree,
	}
}

// Len returns the number of entries in the tree.
func (t *BTree) Len() int {
	return t.length
}

// Has returns true if the key exists in the tree.
func (t *BTree) Has(key int64) bool {
	_, ok := t.Get(key)
	return ok
}

// Get retrieves the value for the given key.
func (t *BTree) Get(key int64) (any, bool) {
	return t.root.get(key)
}

func (n *node) get(key int64) (any, bool) {
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
func (n *node) search(key int64) int {
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
func (t *BTree) Insert(key int64, value any) bool {
	if t.root.has(key) {
		return false
	}
	t.insert(key, value)
	return true
}

func (n *node) has(key int64) bool {
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
func (t *BTree) Put(key int64, value any) {
	if t.root.put(key, value) {
		return
	}
	t.insert(key, value)
}

func (n *node) put(key int64, value any) bool {
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

func (t *BTree) insert(key int64, value any) {
	root := t.root
	if len(root.entries) == 2*t.degree-1 {
		// Root is full, split it
		newRoot := &node{leaf: false}
		newRoot.children = append(newRoot.children, root)
		t.splitChild(newRoot, 0)
		t.root = newRoot
	}
	t.insertNonFull(t.root, entry{key: key, value: value})
	t.length++
}

func (t *BTree) insertNonFull(n *node, e entry) {
	i := n.search(e.key)
	if n.leaf {
		// Insert entry at position i
		n.entries = append(n.entries, entry{})
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

func (t *BTree) splitChild(parent *node, i int) {
	deg := t.degree
	child := parent.children[i]
	mid := deg - 1

	// New node gets the right half
	sibling := &node{leaf: child.leaf}
	sibling.entries = make([]entry, len(child.entries[mid+1:]))
	copy(sibling.entries, child.entries[mid+1:])

	if !child.leaf {
		sibling.children = make([]*node, len(child.children[mid+1:]))
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
	parent.entries = append(parent.entries, entry{})
	copy(parent.entries[i+1:], parent.entries[i:])
	parent.entries[i] = medianEntry
}

// Delete removes a key from the tree. Returns false if the key was not found.
func (t *BTree) Delete(key int64) bool {
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

func (t *BTree) delete(n *node, key int64) bool {
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

func (t *BTree) deleteInternal(n *node, i int) bool {
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

func (t *BTree) deleteFromChild(n *node, i int, key int64) bool {
	child := n.children[i]
	if len(child.entries) < t.degree {
		t.fill(n, i)
		// After fill, structure may have changed (merge/borrow).
		// Re-do delete from this node to find correct path.
		return t.delete(n, key)
	}
	return t.delete(child, key)
}

func (t *BTree) predecessor(n *node) entry {
	for !n.leaf {
		n = n.children[len(n.children)-1]
	}
	return n.entries[len(n.entries)-1]
}

func (t *BTree) successor(n *node) entry {
	for !n.leaf {
		n = n.children[0]
	}
	return n.entries[0]
}

func (t *BTree) fill(parent *node, i int) {
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

func (t *BTree) borrowFromLeft(parent *node, i int) {
	child := parent.children[i]
	left := parent.children[i-1]

	// Shift child entries/children right
	child.entries = append([]entry{parent.entries[i-1]}, child.entries...)
	if !child.leaf {
		child.children = append([]*node{left.children[len(left.children)-1]}, child.children...)
		left.children = left.children[:len(left.children)-1]
	}

	parent.entries[i-1] = left.entries[len(left.entries)-1]
	left.entries = left.entries[:len(left.entries)-1]
}

func (t *BTree) borrowFromRight(parent *node, i int) {
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

func (t *BTree) merge(parent *node, i int) {
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
func (t *BTree) ForEach(fn func(key int64, value any) bool) {
	if t.root != nil {
		t.root.forEach(fn)
	}
}

func (n *node) forEach(fn func(key int64, value any) bool) bool {
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
