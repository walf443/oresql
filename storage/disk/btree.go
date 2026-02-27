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
	leafHdrSize          = 1 + 2 + 4 + 8 // flags(1) + entryCount(2) + nextLeaf(4) + highKey(8)
	internalHdrSize      = 1 + 2         // flags(1) + entryCount(2)
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
}

// NewDiskBTree creates a new empty DiskBTree.
func NewDiskBTree(pool *pager.BufferPool) (*DiskBTree, error) {
	// Allocate root leaf page
	id, data, err := pool.NewPage()
	if err != nil {
		return nil, err
	}
	// Initialize as empty leaf
	data[0] = flagLeaf
	binary.BigEndian.PutUint16(data[1:3], 0)                           // entryCount
	binary.BigEndian.PutUint32(data[3:7], uint32(pager.InvalidPageID)) // nextLeaf
	binary.BigEndian.PutUint64(data[7:15], uint64(math.MaxInt64))      // highKey
	pool.UnpinPage(id, true)

	return &DiskBTree{
		pool:       pool,
		rootPageID: id,
		length:     0,
	}, nil
}

// LoadDiskBTree loads an existing DiskBTree from disk.
func LoadDiskBTree(pool *pager.BufferPool, rootPageID pager.PageID, length int) *DiskBTree {
	return &DiskBTree{
		pool:       pool,
		rootPageID: rootPageID,
		length:     length,
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

// maxLeafEntrySize is a rough check: key(8) + valLen(2) + val(N)
const leafEntryOverhead = 8 + 2

// maxLeafPayload is available space for entries in a leaf page.
const maxLeafPayload = pager.PageSize - leafHdrSize

// maxInternalKeys calculates the max keys in an internal page.
// Each key is 8 bytes, each child is 4 bytes. children = keys + 1.
// internalHdrSize + keys*8 + (keys+1)*4 <= PageSize
// keys*(8+4) + 4 <= PageSize - internalHdrSize
// keys <= (PageSize - internalHdrSize - 4) / 12
const maxInternalKeys = (pager.PageSize - internalHdrSize - 4) / 12

// --- Page encoding/decoding ---

func decodeLeafPage(data []byte) leafPage {
	lp := leafPage{
		entryCount: binary.BigEndian.Uint16(data[1:3]),
		nextLeaf:   binary.BigEndian.Uint32(data[3:7]),
		highKey:    int64(binary.BigEndian.Uint64(data[7:15])),
	}
	pos := leafHdrSize
	lp.entries = make([]leafEntry, lp.entryCount)
	for i := 0; i < int(lp.entryCount); i++ {
		key := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8
		valLen := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2
		val := make([]byte, valLen)
		copy(val, data[pos:pos+int(valLen)])
		pos += int(valLen)
		lp.entries[i] = leafEntry{key: key, valLen: valLen, val: val}
	}
	return lp
}

func encodeLeafPage(lp leafPage, data []byte) {
	for i := range data {
		data[i] = 0
	}
	data[0] = flagLeaf
	binary.BigEndian.PutUint16(data[1:3], lp.entryCount)
	binary.BigEndian.PutUint32(data[3:7], lp.nextLeaf)
	binary.BigEndian.PutUint64(data[7:15], uint64(lp.highKey))
	pos := leafHdrSize
	for _, e := range lp.entries {
		binary.BigEndian.PutUint64(data[pos:pos+8], uint64(e.key))
		pos += 8
		binary.BigEndian.PutUint16(data[pos:pos+2], e.valLen)
		pos += 2
		copy(data[pos:], e.val)
		pos += int(e.valLen)
	}
}

func decodeInternalPage(data []byte) internalPage {
	ip := internalPage{
		entryCount: binary.BigEndian.Uint16(data[1:3]),
	}
	pos := internalHdrSize
	ip.keys = make([]int64, ip.entryCount)
	ip.children = make([]pager.PageID, ip.entryCount+1)

	// First child
	ip.children[0] = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	for i := 0; i < int(ip.entryCount); i++ {
		ip.keys[i] = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8
		ip.children[i+1] = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
	}
	return ip
}

func encodeInternalPage(ip internalPage, data []byte) {
	for i := range data {
		data[i] = 0
	}
	data[0] = 0 // not leaf
	binary.BigEndian.PutUint16(data[1:3], ip.entryCount)
	pos := internalHdrSize
	binary.BigEndian.PutUint32(data[pos:pos+4], ip.children[0])
	pos += 4
	for i := 0; i < int(ip.entryCount); i++ {
		binary.BigEndian.PutUint64(data[pos:pos+8], uint64(ip.keys[i]))
		pos += 8
		binary.BigEndian.PutUint32(data[pos:pos+4], ip.children[i+1])
		pos += 4
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
		ip := decodeInternalPage(data)
		t.pool.UnpinPage(pageID, false)

		// Binary search: find first key > given key
		childIdx := int(ip.entryCount) // default: rightmost child
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
		pageID = ip.children[childIdx]
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
	lp := decodeLeafPage(data)
	t.pool.UnpinPage(leafID, false)

	idx := searchLeaf(lp.entries, key)
	if idx < int(lp.entryCount) && lp.entries[idx].key == key {
		row, err := storage.DecodeRow(lp.entries[idx].val)
		if err != nil {
			return nil, false
		}
		return row, true
	}
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
// This is O(H + L') page fetches instead of O(K × H) for individual Get calls.
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

	for pageID != pager.InvalidPageID && keyIdx < len(sortedKeys) {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return result
		}
		lp := decodeLeafPage(data)
		nextLeaf := lp.nextLeaf
		t.pool.UnpinPage(pageID, false)

		for i := 0; i < int(lp.entryCount) && keyIdx < len(sortedKeys); i++ {
			entryKey := lp.entries[i].key
			for keyIdx < len(sortedKeys) && sortedKeys[keyIdx] < entryKey {
				keyIdx++
			}
			if keyIdx >= len(sortedKeys) {
				break
			}
			if sortedKeys[keyIdx] == entryKey {
				row, err := storage.DecodeRow(lp.entries[i].val)
				if err != nil {
					return result
				}
				result = append(result, storage.KeyRow{Key: entryKey, Row: row})
				keyIdx++
			}
		}
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
		newLP := leafPage{
			entryCount: uint16(len(rightEntries)),
			nextLeaf:   lp.nextLeaf,
			highKey:    lp.highKey,
			entries:    rightEntries,
		}
		encodeLeafPage(newLP, newLeafData)
		t.pool.UnpinPage(newLeafID, true)

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
		ip := decodeInternalPage(data)
		t.pool.UnpinPage(pageID, false)
		pageID = ip.children[0]
	}

	// Traverse leaf chain
	for pageID != pager.InvalidPageID {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		lp := decodeLeafPage(data)
		nextLeaf := lp.nextLeaf
		t.pool.UnpinPage(pageID, false)

		for _, e := range lp.entries {
			row, err := storage.DecodeRow(e.val)
			if err != nil {
				return
			}
			if !fn(e.key, row) {
				return
			}
		}
		pageID = nextLeaf
	}
}

// collectLeafPageIDs traverses from root to the leftmost leaf, then follows
// the leaf chain collecting only PageIDs (no entry decoding).
func (t *DiskBTree) collectLeafPageIDs() []pager.PageID {
	// Find leftmost leaf
	pageID := t.rootPageID
	for {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return nil
		}
		if isLeafPage(data) {
			t.pool.UnpinPage(pageID, false)
			break
		}
		ip := decodeInternalPage(data)
		t.pool.UnpinPage(pageID, false)
		pageID = ip.children[0]
	}

	// Follow leaf chain collecting only PageIDs
	var ids []pager.PageID
	for pageID != pager.InvalidPageID {
		ids = append(ids, pageID)
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return ids
		}
		nextLeaf := pager.PageID(binary.BigEndian.Uint32(data[3:7]))
		t.pool.UnpinPage(pageID, false)
		pageID = nextLeaf
	}
	return ids
}

// ForEachReverse iterates over all entries in reverse key order.
// It collects leaf PageIDs in a forward pass, then traverses them in reverse,
// decoding entries only as needed. This allows early termination (LIMIT)
// without scanning all rows.
func (t *DiskBTree) ForEachReverse(fn func(key int64, row storage.Row) bool) {
	ids := t.collectLeafPageIDs()
	for i := len(ids) - 1; i >= 0; i-- {
		data, err := t.pool.FetchPage(ids[i])
		if err != nil {
			return
		}
		lp := decodeLeafPage(data)
		t.pool.UnpinPage(ids[i], false)
		for j := len(lp.entries) - 1; j >= 0; j-- {
			row, err := storage.DecodeRow(lp.entries[j].val)
			if err != nil {
				return
			}
			if !fn(lp.entries[j].key, row) {
				return
			}
		}
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
			ip := decodeInternalPage(data)
			t.pool.UnpinPage(pageID, false)
			pageID = ip.children[0]
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
		lp := decodeLeafPage(data)
		nextLeaf := lp.nextLeaf
		t.pool.UnpinPage(pageID, false)

		for _, e := range lp.entries {
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
			row, err := storage.DecodeRow(e.val)
			if err != nil {
				return
			}
			if !fn(e.key, row) {
				return
			}
		}
		pageID = nextLeaf
	}
}
