package btree

// StringBTree is a B-tree with string keys and any values.
type StringBTree struct {
	root   *snode
	degree int // minimum degree (t): each node has at most 2t-1 keys
	length int
}

type sentry struct {
	key   string
	value any
}

type snode struct {
	entries  []sentry
	children []*snode
	leaf     bool
}

// NewStringBTree creates a new StringBTree with the given degree (minimum degree t).
// Each node can hold at most 2*degree-1 keys.
func NewStringBTree(degree int) *StringBTree {
	if degree < 2 {
		degree = 2
	}
	return &StringBTree{
		root:   &snode{leaf: true},
		degree: degree,
	}
}

// Len returns the number of entries in the tree.
func (t *StringBTree) Len() int {
	return t.length
}

// Get retrieves the value for the given key.
func (t *StringBTree) Get(key string) (any, bool) {
	return t.root.get(key)
}

func (n *snode) get(key string) (any, bool) {
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
func (n *snode) search(key string) int {
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

// Put inserts or updates a key-value pair (upsert).
func (t *StringBTree) Put(key string, value any) {
	if t.root.put(key, value) {
		return
	}
	t.insert(key, value)
}

func (n *snode) put(key string, value any) bool {
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

func (t *StringBTree) insert(key string, value any) {
	root := t.root
	if len(root.entries) == 2*t.degree-1 {
		newRoot := &snode{leaf: false}
		newRoot.children = append(newRoot.children, root)
		t.splitChild(newRoot, 0)
		t.root = newRoot
	}
	t.insertNonFull(t.root, sentry{key: key, value: value})
	t.length++
}

func (t *StringBTree) insertNonFull(n *snode, e sentry) {
	i := n.search(e.key)
	if n.leaf {
		n.entries = append(n.entries, sentry{})
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

func (t *StringBTree) splitChild(parent *snode, i int) {
	deg := t.degree
	child := parent.children[i]
	mid := deg - 1

	sibling := &snode{leaf: child.leaf}
	sibling.entries = make([]sentry, len(child.entries[mid+1:]))
	copy(sibling.entries, child.entries[mid+1:])

	if !child.leaf {
		sibling.children = make([]*snode, len(child.children[mid+1:]))
		copy(sibling.children, child.children[mid+1:])
	}

	medianEntry := child.entries[mid]

	child.entries = child.entries[:mid]
	if !child.leaf {
		child.children = child.children[:mid+1]
	}

	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = sibling

	parent.entries = append(parent.entries, sentry{})
	copy(parent.entries[i+1:], parent.entries[i:])
	parent.entries[i] = medianEntry
}

// Delete removes a key from the tree. Returns false if the key was not found.
func (t *StringBTree) Delete(key string) bool {
	if t.root == nil {
		return false
	}
	deleted := t.delete(t.root, key)
	if deleted {
		t.length--
		if len(t.root.entries) == 0 && !t.root.leaf {
			t.root = t.root.children[0]
		}
	}
	return deleted
}

func (t *StringBTree) delete(n *snode, key string) bool {
	i := n.search(key)

	if i < len(n.entries) && n.entries[i].key == key {
		if n.leaf {
			n.entries = append(n.entries[:i], n.entries[i+1:]...)
			return true
		}
		return t.deleteInternal(n, i)
	}

	if n.leaf {
		return false
	}

	return t.deleteFromChild(n, i, key)
}

func (t *StringBTree) deleteInternal(n *snode, i int) bool {
	key := n.entries[i].key
	if len(n.children[i].entries) >= t.degree {
		pred := t.predecessor(n.children[i])
		n.entries[i] = pred
		return t.delete(n.children[i], pred.key)
	}
	if len(n.children[i+1].entries) >= t.degree {
		succ := t.successor(n.children[i+1])
		n.entries[i] = succ
		return t.delete(n.children[i+1], succ.key)
	}
	t.merge(n, i)
	return t.delete(n.children[i], key)
}

func (t *StringBTree) deleteFromChild(n *snode, i int, key string) bool {
	child := n.children[i]
	if len(child.entries) < t.degree {
		t.fill(n, i)
		return t.delete(n, key)
	}
	return t.delete(child, key)
}

func (t *StringBTree) predecessor(n *snode) sentry {
	for !n.leaf {
		n = n.children[len(n.children)-1]
	}
	return n.entries[len(n.entries)-1]
}

func (t *StringBTree) successor(n *snode) sentry {
	for !n.leaf {
		n = n.children[0]
	}
	return n.entries[0]
}

func (t *StringBTree) fill(parent *snode, i int) {
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

func (t *StringBTree) borrowFromLeft(parent *snode, i int) {
	child := parent.children[i]
	left := parent.children[i-1]

	child.entries = append([]sentry{parent.entries[i-1]}, child.entries...)
	if !child.leaf {
		child.children = append([]*snode{left.children[len(left.children)-1]}, child.children...)
		left.children = left.children[:len(left.children)-1]
	}

	parent.entries[i-1] = left.entries[len(left.entries)-1]
	left.entries = left.entries[:len(left.entries)-1]
}

func (t *StringBTree) borrowFromRight(parent *snode, i int) {
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

func (t *StringBTree) merge(parent *snode, i int) {
	left := parent.children[i]
	right := parent.children[i+1]

	left.entries = append(left.entries, parent.entries[i])
	left.entries = append(left.entries, right.entries...)
	if !left.leaf {
		left.children = append(left.children, right.children...)
	}

	parent.entries = append(parent.entries[:i], parent.entries[i+1:]...)
	parent.children = append(parent.children[:i+1], parent.children[i+2:]...)
}

// ForEach iterates over all entries in key order.
// The callback should return true to continue, false to stop.
func (t *StringBTree) ForEach(fn func(key string, value any) bool) {
	if t.root != nil {
		t.root.forEach(fn)
	}
}

func (n *snode) forEach(fn func(key string, value any) bool) bool {
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
