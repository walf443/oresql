package disk

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/walf443/oresql/storage"
	"github.com/walf443/oresql/storage/pager"
)

// Page layout constants
const (
	flagLeaf        byte = 0x01
	leafHdrSize          = 1 + 2 + 4 + 4 + 8 // flags(1) + entryCount(2) + nextLeaf(4) + prevLeaf(4) + highKey(8)
	internalHdrSize      = 1 + 2             // flags(1) + entryCount(2)
	entryKeySize         = 8                 // int64 key
	entryValLenSize      = 2                 // uint16 value length
	childSize            = 4                 // uint32 page ID

	// Leaf page header field offsets
	leafOffEntryCount = 1  // uint16 at [1:3]
	leafOffNextLeaf   = 3  // uint32 at [3:7]
	leafOffPrevLeaf   = 7  // uint32 at [7:11]
	leafOffHighKey    = 11 // int64  at [11:19]

	// Internal page header field offset
	internalOffEntryCount = 1 // uint16 at [1:3]
)

// leafEntry is a key-value pair stored in a leaf page.
type leafEntry struct {
	key    int64
	valLen uint16
	val    []byte // EncodeRow format
}

// leafPage represents a decoded leaf page.
type leafPage struct {
	entryCount uint16
	nextLeaf   pager.PageID
	prevLeaf   pager.PageID
	highKey    int64
	entries    []leafEntry
}

// internalPage represents a decoded internal page.
type internalPage struct {
	entryCount uint16
	keys       []int64
	children   []pager.PageID // len = entryCount + 1
}

// DiskBTree is a page-based B+Tree stored on disk via a BufferPool.
type DiskBTree struct {
	pool       *pager.BufferPool
	rootPageID pager.PageID
	length     int
	numCols    int
}

// NewDiskBTree creates a new empty DiskBTree.
func NewDiskBTree(pool *pager.BufferPool, numCols int) (*DiskBTree, error) {
	// Allocate root leaf page
	id, data, err := pool.NewPage()
	if err != nil {
		return nil, err
	}
	// Initialize as empty leaf
	data[0] = flagLeaf
	binary.BigEndian.PutUint16(data[leafOffEntryCount:leafOffEntryCount+2], 0)
	binary.BigEndian.PutUint32(data[leafOffNextLeaf:leafOffNextLeaf+4], uint32(pager.InvalidPageID))
	binary.BigEndian.PutUint32(data[leafOffPrevLeaf:leafOffPrevLeaf+4], uint32(pager.InvalidPageID))
	binary.BigEndian.PutUint64(data[leafOffHighKey:leafOffHighKey+8], uint64(math.MaxInt64))
	pool.UnpinPage(id, true)

	return &DiskBTree{
		pool:       pool,
		rootPageID: id,
		length:     0,
		numCols:    numCols,
	}, nil
}

// LoadDiskBTree loads an existing DiskBTree from disk.
func LoadDiskBTree(pool *pager.BufferPool, rootPageID pager.PageID, length int, numCols int) *DiskBTree {
	return &DiskBTree{
		pool:       pool,
		rootPageID: rootPageID,
		length:     length,
		numCols:    numCols,
	}
}

// RootPageID returns the root page ID.
func (t *DiskBTree) RootPageID() pager.PageID {
	return t.rootPageID
}

// Len returns the number of entries.
func (t *DiskBTree) Len() int {
	return t.length
}

// leafEntryOverhead is the fixed overhead per leaf entry: key + valLen header.
const leafEntryOverhead = entryKeySize + entryValLenSize

// maxLeafPayload is available space for entries in a leaf page.
const maxLeafPayload = pager.PageSize - leafHdrSize

// maxInternalKeys calculates the max keys in an internal page.
// Each key is entryKeySize bytes, each child is childSize bytes. children = keys + 1.
// internalHdrSize + keys*entryKeySize + (keys+1)*childSize <= PageSize
// keys*(entryKeySize+childSize) + childSize <= PageSize - internalHdrSize
// keys <= (PageSize - internalHdrSize - childSize) / (entryKeySize + childSize)
const maxInternalKeys = (pager.PageSize - internalHdrSize - childSize) / (entryKeySize + childSize)

// --- Page encoding/decoding ---

func decodeLeafPage(data []byte) leafPage {
	lp := leafPage{
		entryCount: binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]),
		nextLeaf:   binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]),
		prevLeaf:   binary.BigEndian.Uint32(data[leafOffPrevLeaf : leafOffPrevLeaf+4]),
		highKey:    int64(binary.BigEndian.Uint64(data[leafOffHighKey : leafOffHighKey+8])),
	}
	pos := leafHdrSize
	lp.entries = make([]leafEntry, lp.entryCount)
	for i := 0; i < int(lp.entryCount); i++ {
		key := int64(binary.BigEndian.Uint64(data[pos : pos+entryKeySize]))
		pos += entryKeySize
		valLen := binary.BigEndian.Uint16(data[pos : pos+entryValLenSize])
		pos += entryValLenSize
		val := make([]byte, valLen)
		copy(val, data[pos:pos+int(valLen)])
		pos += int(valLen)
		lp.entries[i] = leafEntry{key: key, valLen: valLen, val: val}
	}
	return lp
}

// decodeLeafHeader reads only the leaf page header (19 bytes) without
// decoding entries. Used to check highKey for skip decisions.
func decodeLeafHeader(data []byte) (entryCount uint16, nextLeaf pager.PageID, highKey int64) {
	entryCount = binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2])
	nextLeaf = binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4])
	highKey = int64(binary.BigEndian.Uint64(data[leafOffHighKey : leafOffHighKey+8]))
	return
}

func encodeLeafPage(lp leafPage, data []byte) {
	for i := range data {
		data[i] = 0
	}
	data[0] = flagLeaf
	binary.BigEndian.PutUint16(data[leafOffEntryCount:leafOffEntryCount+2], lp.entryCount)
	binary.BigEndian.PutUint32(data[leafOffNextLeaf:leafOffNextLeaf+4], lp.nextLeaf)
	binary.BigEndian.PutUint32(data[leafOffPrevLeaf:leafOffPrevLeaf+4], lp.prevLeaf)
	binary.BigEndian.PutUint64(data[leafOffHighKey:leafOffHighKey+8], uint64(lp.highKey))
	pos := leafHdrSize
	for _, e := range lp.entries {
		binary.BigEndian.PutUint64(data[pos:pos+entryKeySize], uint64(e.key))
		pos += entryKeySize
		binary.BigEndian.PutUint16(data[pos:pos+entryValLenSize], e.valLen)
		pos += entryValLenSize
		copy(data[pos:], e.val)
		pos += int(e.valLen)
	}
}

func decodeInternalPage(data []byte) internalPage {
	ip := internalPage{
		entryCount: binary.BigEndian.Uint16(data[internalOffEntryCount : internalOffEntryCount+2]),
	}
	pos := internalHdrSize
	ip.keys = make([]int64, ip.entryCount)
	ip.children = make([]pager.PageID, ip.entryCount+1)

	// First child
	ip.children[0] = binary.BigEndian.Uint32(data[pos : pos+childSize])
	pos += childSize
	for i := 0; i < int(ip.entryCount); i++ {
		ip.keys[i] = int64(binary.BigEndian.Uint64(data[pos : pos+entryKeySize]))
		pos += entryKeySize
		ip.children[i+1] = binary.BigEndian.Uint32(data[pos : pos+childSize])
		pos += childSize
	}
	return ip
}

func encodeInternalPage(ip internalPage, data []byte) {
	for i := range data {
		data[i] = 0
	}
	data[0] = 0 // not leaf
	binary.BigEndian.PutUint16(data[internalOffEntryCount:internalOffEntryCount+2], ip.entryCount)
	pos := internalHdrSize
	binary.BigEndian.PutUint32(data[pos:pos+childSize], ip.children[0])
	pos += childSize
	for i := 0; i < int(ip.entryCount); i++ {
		binary.BigEndian.PutUint64(data[pos:pos+entryKeySize], uint64(ip.keys[i]))
		pos += entryKeySize
		binary.BigEndian.PutUint32(data[pos:pos+childSize], ip.children[i+1])
		pos += childSize
	}
}

func isLeafPage(data []byte) bool {
	return data[0]&flagLeaf != 0
}

// leafPageUsedBytes computes the used bytes for a set of entries.
func leafPageUsedBytes(entries []leafEntry) int {
	total := leafHdrSize
	for _, e := range entries {
		total += leafEntryOverhead + int(e.valLen)
	}
	return total
}

// --- Inline internal page helpers (allocation-free read path) ---

// internalSearchChild performs inline binary search on an internal page buffer
// and returns the child page ID for the given key.
func internalSearchChild(data []byte, key int64) pager.PageID {
	entryCount := int(binary.BigEndian.Uint16(data[internalOffEntryCount : internalOffEntryCount+2]))
	lo, hi := 0, entryCount
	for lo < hi {
		mid := (lo + hi) / 2
		off := internalHdrSize + childSize + mid*(entryKeySize+childSize)
		midKey := int64(binary.BigEndian.Uint64(data[off : off+entryKeySize]))
		if midKey <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	childOff := internalHdrSize + lo*(entryKeySize+childSize)
	return pager.PageID(binary.BigEndian.Uint32(data[childOff : childOff+childSize]))
}

// internalLeftmostChild returns child[0] directly from an internal page buffer.
func internalLeftmostChild(data []byte) pager.PageID {
	return pager.PageID(binary.BigEndian.Uint32(data[internalHdrSize : internalHdrSize+childSize]))
}

// internalRightmostChild returns child[entryCount] directly from an internal page buffer.
func internalRightmostChild(data []byte) pager.PageID {
	entryCount := int(binary.BigEndian.Uint16(data[internalOffEntryCount : internalOffEntryCount+2]))
	off := internalHdrSize + entryCount*(entryKeySize+childSize)
	return pager.PageID(binary.BigEndian.Uint32(data[off : off+childSize]))
}

// --- Search ---

// findLeaf returns the leaf page ID containing the given key.
func (t *DiskBTree) findLeaf(key int64) (pager.PageID, error) {
	pageID := t.rootPageID
	for {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return pager.InvalidPageID, err
		}
		if isLeafPage(data) {
			t.pool.UnpinPage(pageID, false)
			return pageID, nil
		}
		nextPageID := internalSearchChild(data, key)
		t.pool.UnpinPage(pageID, false)
		pageID = nextPageID
	}
}

// Get retrieves the row for the given key.
func (t *DiskBTree) Get(key int64) (storage.Row, bool) {
	leafID, err := t.findLeaf(key)
	if err != nil {
		return nil, false
	}
	data, err := t.pool.FetchPage(leafID)
	if err != nil {
		return nil, false
	}

	entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
	pos := leafHdrSize
	for i := 0; i < entryCount; i++ {
		entryKey := int64(binary.BigEndian.Uint64(data[pos : pos+entryKeySize]))
		pos += entryKeySize
		valLen := int(binary.BigEndian.Uint16(data[pos : pos+entryValLenSize]))
		pos += entryValLenSize
		if entryKey == key {
			row, err := storage.DecodeRowN(data[pos:pos+valLen], t.numCols)
			t.pool.UnpinPage(leafID, false)
			if err != nil {
				return nil, false
			}
			return row, true
		}
		if entryKey > key {
			break // エントリはキー昇順なのでこれ以降にはない
		}
		pos += valLen
	}
	t.pool.UnpinPage(leafID, false)
	return nil, false
}

// Has returns true if the key exists.
func (t *DiskBTree) Has(key int64) bool {
	_, ok := t.Get(key)
	return ok
}

// searchLeaf returns the index of the first entry with key >= given key.
func searchLeaf(entries []leafEntry, key int64) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if entries[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// GetByKeysSorted retrieves rows for pre-sorted keys using a single findLeaf
// call followed by a leaf-chain traversal with two-pointer matching.
// Optimized with header-only skip (highKey check) and gap jump (findLeaf after
// consecutive skips) to avoid decoding pages that cannot contain any queried keys.
func (t *DiskBTree) GetByKeysSorted(sortedKeys []int64) []storage.KeyRow {
	if len(sortedKeys) == 0 {
		return nil
	}
	leafID, err := t.findLeaf(sortedKeys[0])
	if err != nil {
		return nil
	}
	result := make([]storage.KeyRow, 0, len(sortedKeys))
	keyIdx := 0
	pageID := leafID

	const maxConsecutiveSkips = 4
	consecutiveSkips := 0

	for pageID != pager.InvalidPageID && keyIdx < len(sortedKeys) {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return result
		}

		// Read header only to check highKey
		_, hdrNextLeaf, highKey := decodeLeafHeader(data)

		if sortedKeys[keyIdx] >= highKey {
			// No match possible — skip without decoding entries
			t.pool.UnpinPage(pageID, false)
			consecutiveSkips++

			if consecutiveSkips > maxConsecutiveSkips {
				// Gap jump: use findLeaf to go directly to the target leaf
				jumpTarget, err := t.findLeaf(sortedKeys[keyIdx])
				if err != nil {
					return result
				}
				pageID = jumpTarget
				consecutiveSkips = 0
			} else {
				pageID = hdrNextLeaf
			}
			continue
		}

		// Match possible — scan entries inline, decode only matching vals
		entryCount, nextLeaf, _ := decodeLeafHeader(data)
		consecutiveSkips = 0

		pos := leafHdrSize
		for i := 0; i < int(entryCount) && keyIdx < len(sortedKeys); i++ {
			entryKey := int64(binary.BigEndian.Uint64(data[pos : pos+entryKeySize]))
			pos += entryKeySize
			valLen := int(binary.BigEndian.Uint16(data[pos : pos+entryValLenSize]))
			pos += entryValLenSize

			for keyIdx < len(sortedKeys) && sortedKeys[keyIdx] < entryKey {
				keyIdx++
			}
			if keyIdx >= len(sortedKeys) {
				break
			}
			if sortedKeys[keyIdx] == entryKey {
				// DecodeRowN while page is pinned — safe because DecodeRowN copies all values
				row, err := storage.DecodeRowN(data[pos:pos+valLen], t.numCols)
				if err != nil {
					t.pool.UnpinPage(pageID, false)
					return result
				}
				result = append(result, storage.KeyRow{Key: entryKey, Row: row})
				keyIdx++
			}
			pos += valLen
		}
		t.pool.UnpinPage(pageID, false)
		pageID = nextLeaf
	}
	return result
}

// --- Insert ---

// Insert inserts a key-row pair. Returns false if the key already exists.
func (t *DiskBTree) Insert(key int64, row storage.Row) bool {
	if t.Has(key) {
		return false
	}
	t.Put(key, row)
	return true
}

// Put inserts or updates a key-row pair (upsert).
func (t *DiskBTree) Put(key int64, row storage.Row) {
	encoded := storage.EncodeRow(row)
	if len(encoded)+leafEntryOverhead > maxLeafPayload {
		panic(fmt.Sprintf("disk btree: row too large: %d bytes (max ~%d)", len(encoded), maxLeafPayload-leafEntryOverhead))
	}

	entry := leafEntry{key: key, valLen: uint16(len(encoded)), val: encoded}

	// Find the leaf
	leafID, err := t.findLeaf(key)
	if err != nil {
		panic(fmt.Sprintf("disk btree put: %v", err))
	}

	data, err := t.pool.FetchPage(leafID)
	if err != nil {
		panic(fmt.Sprintf("disk btree put fetch: %v", err))
	}
	lp := decodeLeafPage(data)

	// Check for existing key (upsert)
	idx := searchLeaf(lp.entries, key)
	isUpdate := idx < int(lp.entryCount) && lp.entries[idx].key == key
	if isUpdate {
		lp.entries[idx] = entry
		encodeLeafPage(lp, data)
		t.pool.UnpinPage(leafID, true)
		return
	}

	// Insert new entry
	lp.entries = append(lp.entries, leafEntry{})
	copy(lp.entries[idx+1:], lp.entries[idx:])
	lp.entries[idx] = entry
	lp.entryCount++

	// Check if page needs splitting
	if leafPageUsedBytes(lp.entries) <= pager.PageSize {
		encodeLeafPage(lp, data)
		t.pool.UnpinPage(leafID, true)
		t.length++
		return
	}

	t.pool.UnpinPage(leafID, false)
	t.length++

	// Need to split - use recursive insert from root
	t.insertFromRoot(entry)
}

// insertFromRoot handles splitting by doing a top-down insert.
func (t *DiskBTree) insertFromRoot(entry leafEntry) {
	splitKey, splitChild := t.insertRecursive(t.rootPageID, entry)
	if splitChild == pager.InvalidPageID {
		return
	}
	// Root was split - create new root
	newRootID, newRootData, err := t.pool.NewPage()
	if err != nil {
		panic(fmt.Sprintf("disk btree: alloc new root: %v", err))
	}
	ip := internalPage{
		entryCount: 1,
		keys:       []int64{splitKey},
		children:   []pager.PageID{t.rootPageID, splitChild},
	}
	encodeInternalPage(ip, newRootData)
	t.pool.UnpinPage(newRootID, true)
	t.rootPageID = newRootID
}

// insertRecursive inserts into the subtree rooted at pageID.
// Returns (splitKey, newChildPageID) if the node was split, or (0, InvalidPageID) if not.
func (t *DiskBTree) insertRecursive(pageID pager.PageID, entry leafEntry) (int64, pager.PageID) {
	data, err := t.pool.FetchPage(pageID)
	if err != nil {
		panic(fmt.Sprintf("disk btree insert recursive: %v", err))
	}

	if isLeafPage(data) {
		lp := decodeLeafPage(data)
		idx := searchLeaf(lp.entries, entry.key)

		// Insert
		lp.entries = append(lp.entries, leafEntry{})
		copy(lp.entries[idx+1:], lp.entries[idx:])
		lp.entries[idx] = entry
		lp.entryCount++

		if leafPageUsedBytes(lp.entries) <= pager.PageSize {
			encodeLeafPage(lp, data)
			t.pool.UnpinPage(pageID, true)
			return 0, pager.InvalidPageID
		}

		// Split leaf
		mid := int(lp.entryCount) / 2
		newLeafID, newLeafData, err := t.pool.NewPage()
		if err != nil {
			panic(fmt.Sprintf("disk btree: alloc new leaf: %v", err))
		}

		rightEntries := make([]leafEntry, len(lp.entries[mid:]))
		copy(rightEntries, lp.entries[mid:])
		leftEntries := make([]leafEntry, mid)
		copy(leftEntries, lp.entries[:mid])

		// New right leaf
		oldNext := lp.nextLeaf
		newLP := leafPage{
			entryCount: uint16(len(rightEntries)),
			nextLeaf:   oldNext,
			prevLeaf:   pageID,
			highKey:    lp.highKey,
			entries:    rightEntries,
		}
		encodeLeafPage(newLP, newLeafData)
		t.pool.UnpinPage(newLeafID, true)

		// Update prevLeaf of the old next leaf
		if oldNext != pager.InvalidPageID {
			oldNextData, err := t.pool.FetchPage(oldNext)
			if err == nil {
				binary.BigEndian.PutUint32(oldNextData[7:11], newLeafID)
				t.pool.UnpinPage(oldNext, true)
			}
		}

		// Update left leaf
		lp.entries = leftEntries
		lp.entryCount = uint16(mid)
		lp.nextLeaf = newLeafID
		lp.highKey = rightEntries[0].key
		encodeLeafPage(lp, data)
		t.pool.UnpinPage(pageID, true)

		return rightEntries[0].key, newLeafID
	}

	// Internal node
	ip := decodeInternalPage(data)
	t.pool.UnpinPage(pageID, false)

	// Find child
	childIdx := int(ip.entryCount)
	lo, hi := 0, int(ip.entryCount)
	for lo < hi {
		mid := (lo + hi) / 2
		if ip.keys[mid] <= entry.key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	childIdx = lo

	splitKey, splitChild := t.insertRecursive(ip.children[childIdx], entry)
	if splitChild == pager.InvalidPageID {
		return 0, pager.InvalidPageID
	}

	// Insert the new separator key and child
	ip.keys = append(ip.keys, 0)
	copy(ip.keys[childIdx+1:], ip.keys[childIdx:])
	ip.keys[childIdx] = splitKey
	ip.children = append(ip.children, 0)
	copy(ip.children[childIdx+2:], ip.children[childIdx+1:])
	ip.children[childIdx+1] = splitChild
	ip.entryCount++

	if int(ip.entryCount) <= maxInternalKeys {
		// Re-fetch and write
		data2, err := t.pool.FetchPage(pageID)
		if err != nil {
			panic(fmt.Sprintf("disk btree: re-fetch internal: %v", err))
		}
		encodeInternalPage(ip, data2)
		t.pool.UnpinPage(pageID, true)
		return 0, pager.InvalidPageID
	}

	// Split internal node
	mid := int(ip.entryCount) / 2
	promoteKey := ip.keys[mid]

	newInternalID, newInternalData, err := t.pool.NewPage()
	if err != nil {
		panic(fmt.Sprintf("disk btree: alloc new internal: %v", err))
	}

	rightIP := internalPage{
		entryCount: uint16(int(ip.entryCount) - mid - 1),
		keys:       make([]int64, int(ip.entryCount)-mid-1),
		children:   make([]pager.PageID, int(ip.entryCount)-mid),
	}
	copy(rightIP.keys, ip.keys[mid+1:])
	copy(rightIP.children, ip.children[mid+1:])
	encodeInternalPage(rightIP, newInternalData)
	t.pool.UnpinPage(newInternalID, true)

	leftIP := internalPage{
		entryCount: uint16(mid),
		keys:       ip.keys[:mid],
		children:   ip.children[:mid+1],
	}
	data2, err := t.pool.FetchPage(pageID)
	if err != nil {
		panic(fmt.Sprintf("disk btree: re-fetch for split: %v", err))
	}
	encodeInternalPage(leftIP, data2)
	t.pool.UnpinPage(pageID, true)

	return promoteKey, newInternalID
}

// --- Delete ---

// Delete removes a key. Returns false if not found.
func (t *DiskBTree) Delete(key int64) bool {
	deleted := t.deleteRecursive(t.rootPageID, key)
	if !deleted {
		return false
	}
	t.length--

	// Check if root became empty internal node
	data, err := t.pool.FetchPage(t.rootPageID)
	if err != nil {
		return true
	}
	if !isLeafPage(data) {
		ip := decodeInternalPage(data)
		if ip.entryCount == 0 {
			oldRoot := t.rootPageID
			t.rootPageID = ip.children[0]
			t.pool.UnpinPage(oldRoot, false)
			return true
		}
	}
	t.pool.UnpinPage(t.rootPageID, false)
	return true
}

func (t *DiskBTree) deleteRecursive(pageID pager.PageID, key int64) bool {
	data, err := t.pool.FetchPage(pageID)
	if err != nil {
		return false
	}

	if isLeafPage(data) {
		lp := decodeLeafPage(data)
		idx := searchLeaf(lp.entries, key)
		if idx >= int(lp.entryCount) || lp.entries[idx].key != key {
			t.pool.UnpinPage(pageID, false)
			return false
		}
		lp.entries = append(lp.entries[:idx], lp.entries[idx+1:]...)
		lp.entryCount--
		encodeLeafPage(lp, data)
		t.pool.UnpinPage(pageID, true)
		return true
	}

	// Internal node
	ip := decodeInternalPage(data)
	t.pool.UnpinPage(pageID, false)

	// Find child
	childIdx := int(ip.entryCount)
	lo, hi := 0, int(ip.entryCount)
	for lo < hi {
		mid := (lo + hi) / 2
		if ip.keys[mid] <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	childIdx = lo

	// Check if child needs filling (min entries check)
	childData, err := t.pool.FetchPage(ip.children[childIdx])
	if err != nil {
		return false
	}
	needsFill := false
	if isLeafPage(childData) {
		clp := decodeLeafPage(childData)
		// A leaf is "underfull" if it has very few entries.
		// Use a simple threshold: at most 1 entry (need to borrow/merge)
		// For simplicity, we handle rebalancing at minimum 1 entry.
		if clp.entryCount <= 1 && t.length > 2 {
			needsFill = true
		}
	} else {
		cip := decodeInternalPage(childData)
		if cip.entryCount < 1 {
			needsFill = true
		}
	}
	t.pool.UnpinPage(ip.children[childIdx], false)

	if needsFill {
		t.fillChild(pageID, &ip, childIdx)
		// Re-read internal page after fill (structure may have changed)
		data2, err := t.pool.FetchPage(pageID)
		if err != nil {
			return false
		}
		if isLeafPage(data2) {
			// Root became leaf after merge
			lp := decodeLeafPage(data2)
			idx := searchLeaf(lp.entries, key)
			if idx >= int(lp.entryCount) || lp.entries[idx].key != key {
				t.pool.UnpinPage(pageID, false)
				return false
			}
			lp.entries = append(lp.entries[:idx], lp.entries[idx+1:]...)
			lp.entryCount--
			encodeLeafPage(lp, data2)
			t.pool.UnpinPage(pageID, true)
			return true
		}
		ip = decodeInternalPage(data2)
		t.pool.UnpinPage(pageID, false)

		// Re-find child
		childIdx = int(ip.entryCount)
		lo, hi = 0, int(ip.entryCount)
		for lo < hi {
			mid := (lo + hi) / 2
			if ip.keys[mid] <= key {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		childIdx = lo
		if childIdx > int(ip.entryCount) {
			childIdx = int(ip.entryCount)
		}
	}

	deleted := t.deleteRecursive(ip.children[childIdx], key)

	// Update separator keys if needed
	if deleted {
		t.updateSeparatorKeys(pageID, key)
	}

	return deleted
}

// updateSeparatorKeys fixes separator keys after deletion.
func (t *DiskBTree) updateSeparatorKeys(pageID pager.PageID, deletedKey int64) {
	data, err := t.pool.FetchPage(pageID)
	if err != nil {
		return
	}
	if isLeafPage(data) {
		t.pool.UnpinPage(pageID, false)
		return
	}
	ip := decodeInternalPage(data)
	changed := false
	for i := 0; i < int(ip.entryCount); i++ {
		if ip.keys[i] == deletedKey {
			// Find min key in right subtree
			minKey := t.findMinKey(ip.children[i+1])
			ip.keys[i] = minKey
			changed = true
		}
	}
	if changed {
		encodeInternalPage(ip, data)
		t.pool.UnpinPage(pageID, true)
	} else {
		t.pool.UnpinPage(pageID, false)
	}
}

func (t *DiskBTree) findMinKey(pageID pager.PageID) int64 {
	data, err := t.pool.FetchPage(pageID)
	if err != nil {
		return 0
	}
	if isLeafPage(data) {
		lp := decodeLeafPage(data)
		t.pool.UnpinPage(pageID, false)
		if lp.entryCount > 0 {
			return lp.entries[0].key
		}
		return 0
	}
	ip := decodeInternalPage(data)
	t.pool.UnpinPage(pageID, false)
	return t.findMinKey(ip.children[0])
}

func (t *DiskBTree) fillChild(parentID pager.PageID, ip *internalPage, childIdx int) {
	// Try borrow from left sibling
	if childIdx > 0 {
		leftData, err := t.pool.FetchPage(ip.children[childIdx-1])
		if err == nil {
			if isLeafPage(leftData) {
				leftLP := decodeLeafPage(leftData)
				t.pool.UnpinPage(ip.children[childIdx-1], false)
				if leftLP.entryCount > 1 {
					t.borrowFromLeftLeaf(parentID, ip, childIdx)
					return
				}
			} else {
				leftIP := decodeInternalPage(leftData)
				t.pool.UnpinPage(ip.children[childIdx-1], false)
				if leftIP.entryCount > 1 {
					t.borrowFromLeftInternal(parentID, ip, childIdx)
					return
				}
			}
		}
	}

	// Try borrow from right sibling
	if childIdx < int(ip.entryCount) {
		rightData, err := t.pool.FetchPage(ip.children[childIdx+1])
		if err == nil {
			if isLeafPage(rightData) {
				rightLP := decodeLeafPage(rightData)
				t.pool.UnpinPage(ip.children[childIdx+1], false)
				if rightLP.entryCount > 1 {
					t.borrowFromRightLeaf(parentID, ip, childIdx)
					return
				}
			} else {
				rightIP := decodeInternalPage(rightData)
				t.pool.UnpinPage(ip.children[childIdx+1], false)
				if rightIP.entryCount > 1 {
					t.borrowFromRightInternal(parentID, ip, childIdx)
					return
				}
			}
		}
	}

	// Merge
	if childIdx < int(ip.entryCount) {
		t.mergeChildren(parentID, ip, childIdx)
	} else if childIdx > 0 {
		t.mergeChildren(parentID, ip, childIdx-1)
	}
}

func (t *DiskBTree) borrowFromLeftLeaf(parentID pager.PageID, ip *internalPage, childIdx int) {
	leftData, _ := t.pool.FetchPage(ip.children[childIdx-1])
	leftLP := decodeLeafPage(leftData)

	childData, _ := t.pool.FetchPage(ip.children[childIdx])
	childLP := decodeLeafPage(childData)

	// Move last entry from left to front of child
	moved := leftLP.entries[leftLP.entryCount-1]
	leftLP.entries = leftLP.entries[:leftLP.entryCount-1]
	leftLP.entryCount--

	childLP.entries = append([]leafEntry{moved}, childLP.entries...)
	childLP.entryCount++

	encodeLeafPage(leftLP, leftData)
	t.pool.UnpinPage(ip.children[childIdx-1], true)

	encodeLeafPage(childLP, childData)
	t.pool.UnpinPage(ip.children[childIdx], true)

	// Update separator in parent
	ip.keys[childIdx-1] = childLP.entries[0].key
	parentData, _ := t.pool.FetchPage(parentID)
	encodeInternalPage(*ip, parentData)
	t.pool.UnpinPage(parentID, true)
}

func (t *DiskBTree) borrowFromRightLeaf(parentID pager.PageID, ip *internalPage, childIdx int) {
	childData, _ := t.pool.FetchPage(ip.children[childIdx])
	childLP := decodeLeafPage(childData)

	rightData, _ := t.pool.FetchPage(ip.children[childIdx+1])
	rightLP := decodeLeafPage(rightData)

	// Move first entry from right to end of child
	moved := rightLP.entries[0]
	rightLP.entries = rightLP.entries[1:]
	rightLP.entryCount--

	childLP.entries = append(childLP.entries, moved)
	childLP.entryCount++

	encodeLeafPage(childLP, childData)
	t.pool.UnpinPage(ip.children[childIdx], true)

	encodeLeafPage(rightLP, rightData)
	t.pool.UnpinPage(ip.children[childIdx+1], true)

	// Update separator
	ip.keys[childIdx] = rightLP.entries[0].key
	parentData, _ := t.pool.FetchPage(parentID)
	encodeInternalPage(*ip, parentData)
	t.pool.UnpinPage(parentID, true)
}

func (t *DiskBTree) borrowFromLeftInternal(parentID pager.PageID, ip *internalPage, childIdx int) {
	leftData, _ := t.pool.FetchPage(ip.children[childIdx-1])
	leftIP := decodeInternalPage(leftData)

	childData, _ := t.pool.FetchPage(ip.children[childIdx])
	childIP := decodeInternalPage(childData)

	// Rotate through parent
	childIP.keys = append([]int64{ip.keys[childIdx-1]}, childIP.keys...)
	childIP.children = append([]pager.PageID{leftIP.children[leftIP.entryCount]}, childIP.children...)
	childIP.entryCount++

	ip.keys[childIdx-1] = leftIP.keys[leftIP.entryCount-1]
	leftIP.keys = leftIP.keys[:leftIP.entryCount-1]
	leftIP.children = leftIP.children[:leftIP.entryCount]
	leftIP.entryCount--

	encodeInternalPage(leftIP, leftData)
	t.pool.UnpinPage(ip.children[childIdx-1], true)

	encodeInternalPage(childIP, childData)
	t.pool.UnpinPage(ip.children[childIdx], true)

	parentData, _ := t.pool.FetchPage(parentID)
	encodeInternalPage(*ip, parentData)
	t.pool.UnpinPage(parentID, true)
}

func (t *DiskBTree) borrowFromRightInternal(parentID pager.PageID, ip *internalPage, childIdx int) {
	childData, _ := t.pool.FetchPage(ip.children[childIdx])
	childIP := decodeInternalPage(childData)

	rightData, _ := t.pool.FetchPage(ip.children[childIdx+1])
	rightIP := decodeInternalPage(rightData)

	// Rotate through parent
	childIP.keys = append(childIP.keys, ip.keys[childIdx])
	childIP.children = append(childIP.children, rightIP.children[0])
	childIP.entryCount++

	ip.keys[childIdx] = rightIP.keys[0]
	rightIP.keys = rightIP.keys[1:]
	rightIP.children = rightIP.children[1:]
	rightIP.entryCount--

	encodeInternalPage(childIP, childData)
	t.pool.UnpinPage(ip.children[childIdx], true)

	encodeInternalPage(rightIP, rightData)
	t.pool.UnpinPage(ip.children[childIdx+1], true)

	parentData, _ := t.pool.FetchPage(parentID)
	encodeInternalPage(*ip, parentData)
	t.pool.UnpinPage(parentID, true)
}

func (t *DiskBTree) mergeChildren(parentID pager.PageID, ip *internalPage, leftIdx int) {
	leftData, _ := t.pool.FetchPage(ip.children[leftIdx])
	rightData, _ := t.pool.FetchPage(ip.children[leftIdx+1])

	if isLeafPage(leftData) {
		leftLP := decodeLeafPage(leftData)
		rightLP := decodeLeafPage(rightData)

		leftLP.entries = append(leftLP.entries, rightLP.entries...)
		leftLP.entryCount = uint16(len(leftLP.entries))
		leftLP.nextLeaf = rightLP.nextLeaf
		leftLP.highKey = rightLP.highKey

		encodeLeafPage(leftLP, leftData)
		t.pool.UnpinPage(ip.children[leftIdx], true)
		t.pool.UnpinPage(ip.children[leftIdx+1], false)

		// Update prevLeaf of the page after right
		afterRight := rightLP.nextLeaf
		if afterRight != pager.InvalidPageID {
			afterRightData, err := t.pool.FetchPage(afterRight)
			if err == nil {
				binary.BigEndian.PutUint32(afterRightData[7:11], ip.children[leftIdx])
				t.pool.UnpinPage(afterRight, true)
			}
		}
	} else {
		leftIP := decodeInternalPage(leftData)
		rightIP := decodeInternalPage(rightData)

		// Bring down separator from parent
		leftIP.keys = append(leftIP.keys, ip.keys[leftIdx])
		leftIP.keys = append(leftIP.keys, rightIP.keys...)
		leftIP.children = append(leftIP.children, rightIP.children...)
		leftIP.entryCount = uint16(len(leftIP.keys))

		encodeInternalPage(leftIP, leftData)
		t.pool.UnpinPage(ip.children[leftIdx], true)
		t.pool.UnpinPage(ip.children[leftIdx+1], false)
	}

	// Remove the separator and right child from parent
	ip.keys = append(ip.keys[:leftIdx], ip.keys[leftIdx+1:]...)
	ip.children = append(ip.children[:leftIdx+1], ip.children[leftIdx+2:]...)
	ip.entryCount--

	parentData, _ := t.pool.FetchPage(parentID)
	encodeInternalPage(*ip, parentData)
	t.pool.UnpinPage(parentID, true)
}

// --- Iteration ---

// ForEach iterates over all entries in key order.
func (t *DiskBTree) ForEach(fn func(key int64, row storage.Row) bool) {
	// Find leftmost leaf
	pageID := t.rootPageID
	for {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		if isLeafPage(data) {
			t.pool.UnpinPage(pageID, false)
			break
		}
		nextPageID := internalLeftmostChild(data)
		t.pool.UnpinPage(pageID, false)
		pageID = nextPageID
	}

	// Traverse leaf chain with inline page scanning
	for pageID != pager.InvalidPageID {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		nextLeaf := pager.PageID(binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]))

		pos := leafHdrSize
		stopped := false
		for i := 0; i < entryCount; i++ {
			entryKey := int64(binary.BigEndian.Uint64(data[pos : pos+entryKeySize]))
			pos += entryKeySize
			valLen := int(binary.BigEndian.Uint16(data[pos : pos+entryValLenSize]))
			pos += entryValLenSize
			row, err := storage.DecodeRowN(data[pos:pos+valLen], t.numCols)
			if err != nil {
				t.pool.UnpinPage(pageID, false)
				return
			}
			if !fn(entryKey, row) {
				stopped = true
				break
			}
			pos += valLen
		}
		t.pool.UnpinPage(pageID, false)
		if stopped {
			return
		}
		pageID = nextLeaf
	}
}

// ForEachKeyOnly iterates over all entries in key order, passing only the key
// to the callback. Value bytes are skipped without decoding (no DecodeRowN),
// making this ideal for PK-covering queries that only need the primary key.
func (t *DiskBTree) ForEachKeyOnly(fn func(key int64) bool) {
	// Find leftmost leaf
	pageID := t.rootPageID
	for {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		if isLeafPage(data) {
			t.pool.UnpinPage(pageID, false)
			break
		}
		nextPageID := internalLeftmostChild(data)
		t.pool.UnpinPage(pageID, false)
		pageID = nextPageID
	}

	// Traverse leaf chain, skipping value bytes
	for pageID != pager.InvalidPageID {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		nextLeaf := pager.PageID(binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]))

		pos := leafHdrSize
		stopped := false
		for i := 0; i < entryCount; i++ {
			entryKey := int64(binary.BigEndian.Uint64(data[pos : pos+entryKeySize]))
			pos += entryKeySize
			valLen := int(binary.BigEndian.Uint16(data[pos : pos+entryValLenSize]))
			pos += entryValLenSize
			if !fn(entryKey) {
				stopped = true
				break
			}
			pos += valLen // skip value bytes
		}
		t.pool.UnpinPage(pageID, false)
		if stopped {
			return
		}
		pageID = nextLeaf
	}
}

// ForEachKeyOnlyReverse iterates over all entries in reverse key order,
// passing only the key to the callback. Value bytes are skipped without decoding.
func (t *DiskBTree) ForEachKeyOnlyReverse(fn func(key int64) bool) {
	pageID, err := t.findRightmostLeaf()
	if err != nil {
		return
	}
	for pageID != pager.InvalidPageID {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		prevLeaf := pager.PageID(binary.BigEndian.Uint32(data[leafOffPrevLeaf : leafOffPrevLeaf+4]))

		// Collect key offsets in forward pass (variable-length entries)
		keyOffsets := make([]int, entryCount)
		pos := leafHdrSize
		for i := 0; i < entryCount; i++ {
			keyOffsets[i] = pos
			pos += entryKeySize
			valLen := int(binary.BigEndian.Uint16(data[pos : pos+entryValLenSize]))
			pos += entryValLenSize + valLen
		}

		// Scan keys in reverse order
		stopped := false
		for j := entryCount - 1; j >= 0; j-- {
			entryKey := int64(binary.BigEndian.Uint64(data[keyOffsets[j] : keyOffsets[j]+entryKeySize]))
			if !fn(entryKey) {
				stopped = true
				break
			}
		}
		t.pool.UnpinPage(pageID, false)
		if stopped {
			return
		}
		pageID = prevLeaf
	}
}

// findRightmostLeaf returns the PageID of the rightmost leaf page.
func (t *DiskBTree) findRightmostLeaf() (pager.PageID, error) {
	pageID := t.rootPageID
	for {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return pager.InvalidPageID, err
		}
		if isLeafPage(data) {
			t.pool.UnpinPage(pageID, false)
			return pageID, nil
		}
		nextPageID := internalRightmostChild(data)
		t.pool.UnpinPage(pageID, false)
		pageID = nextPageID
	}
}

// ForEachReverse iterates over all entries in reverse key order.
// It finds the rightmost leaf and follows prevLeaf back-pointers, allowing
// early termination (LIMIT) with O(H + needed leaves) page fetches.
func (t *DiskBTree) ForEachReverse(fn func(key int64, row storage.Row) bool) {
	pageID, err := t.findRightmostLeaf()
	if err != nil {
		return
	}
	for pageID != pager.InvalidPageID {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		prevLeaf := pager.PageID(binary.BigEndian.Uint32(data[leafOffPrevLeaf : leafOffPrevLeaf+4]))

		// Collect entry offsets in forward pass (variable-length entries)
		offsets := make([]int, entryCount)
		pos := leafHdrSize
		for i := 0; i < entryCount; i++ {
			offsets[i] = pos
			pos += entryKeySize
			valLen := int(binary.BigEndian.Uint16(data[pos : pos+entryValLenSize]))
			pos += entryValLenSize + valLen
		}

		// Scan entries in reverse order
		stopped := false
		for j := entryCount - 1; j >= 0; j-- {
			off := offsets[j]
			entryKey := int64(binary.BigEndian.Uint64(data[off : off+entryKeySize]))
			valLen := int(binary.BigEndian.Uint16(data[off+entryKeySize : off+entryKeySize+entryValLenSize]))
			valOff := off + entryKeySize + entryValLenSize
			row, err := storage.DecodeRowN(data[valOff:valOff+valLen], t.numCols)
			if err != nil {
				t.pool.UnpinPage(pageID, false)
				return
			}
			if !fn(entryKey, row) {
				stopped = true
				break
			}
		}
		t.pool.UnpinPage(pageID, false)
		if stopped {
			return
		}
		pageID = prevLeaf
	}
}

// ForEachRange iterates over entries whose keys fall within the given range.
func (t *DiskBTree) ForEachRange(from *int64, fromInclusive bool, to *int64, toInclusive bool, fn func(key int64, row storage.Row) bool) {
	var startPageID pager.PageID
	if from == nil {
		// Start from leftmost leaf
		pageID := t.rootPageID
		for {
			data, err := t.pool.FetchPage(pageID)
			if err != nil {
				return
			}
			if isLeafPage(data) {
				t.pool.UnpinPage(pageID, false)
				break
			}
			nextPageID := internalLeftmostChild(data)
			t.pool.UnpinPage(pageID, false)
			pageID = nextPageID
		}
		startPageID = pageID
	} else {
		var err error
		startPageID, err = t.findLeaf(*from)
		if err != nil {
			return
		}
	}

	for pageID := startPageID; pageID != pager.InvalidPageID; {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		nextLeaf := pager.PageID(binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]))

		pos := leafHdrSize
		stopped := false
		for i := 0; i < entryCount; i++ {
			entryKey := int64(binary.BigEndian.Uint64(data[pos : pos+entryKeySize]))
			pos += entryKeySize
			valLen := int(binary.BigEndian.Uint16(data[pos : pos+entryValLenSize]))
			pos += entryValLenSize
			// from boundary check — skip entries before range
			if from != nil {
				if fromInclusive {
					if entryKey < *from {
						pos += valLen
						continue
					}
				} else {
					if entryKey <= *from {
						pos += valLen
						continue
					}
				}
			}
			// to boundary check — stop when past range
			if to != nil {
				if toInclusive {
					if entryKey > *to {
						stopped = true
						break
					}
				} else {
					if entryKey >= *to {
						stopped = true
						break
					}
				}
			}
			row, err := storage.DecodeRowN(data[pos:pos+valLen], t.numCols)
			if err != nil {
				t.pool.UnpinPage(pageID, false)
				return
			}
			if !fn(entryKey, row) {
				stopped = true
				break
			}
			pos += valLen
		}
		t.pool.UnpinPage(pageID, false)
		if stopped {
			return
		}
		pageID = nextLeaf
	}
}
