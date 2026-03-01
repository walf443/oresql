package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/walf443/oresql/storage/pager"
)

// =============================================================================
// DiskSecondaryBTree — variable-length key B+Tree for secondary indexes
// =============================================================================

// Secondary index leaf page layout:
//
//	[flags: 1B]          // 0x01 (leaf)
//	[entryCount: 2B]
//	[nextLeaf: 4B]       // PageID
//	[prevLeaf: 4B]       // PageID
//	entries:
//	  [keyLen: 2B][key: N bytes]  // compositeKey
const secLeafHdrSize = 1 + 2 + 4 + 4 // flags(1) + entryCount(2) + nextLeaf(4) + prevLeaf(4)

type secLeafEntry struct {
	key []byte
}

type secLeafPage struct {
	entryCount uint16
	nextLeaf   pager.PageID
	prevLeaf   pager.PageID
	entries    []secLeafEntry
}

type secInternalPage struct {
	entryCount uint16
	keys       [][]byte
	children   []pager.PageID // len = entryCount + 1
}

// DiskSecondaryBTree is a page-based B+Tree with variable-length keys.
type DiskSecondaryBTree struct {
	pool       *pager.BufferPool
	rootPageID pager.PageID
	length     int
}

// NewDiskSecondaryBTree creates a new empty DiskSecondaryBTree.
func NewDiskSecondaryBTree(pool *pager.BufferPool) (*DiskSecondaryBTree, error) {
	id, data, err := pool.NewPage()
	if err != nil {
		return nil, err
	}
	data[0] = flagLeaf
	binary.BigEndian.PutUint16(data[leafOffEntryCount:leafOffEntryCount+2], 0)
	binary.BigEndian.PutUint32(data[leafOffNextLeaf:leafOffNextLeaf+4], uint32(pager.InvalidPageID))
	binary.BigEndian.PutUint32(data[leafOffPrevLeaf:leafOffPrevLeaf+4], uint32(pager.InvalidPageID))
	pool.UnpinPage(id, true)

	return &DiskSecondaryBTree{
		pool:       pool,
		rootPageID: id,
		length:     0,
	}, nil
}

// LoadDiskSecondaryBTree loads an existing secondary BTree.
func LoadDiskSecondaryBTree(pool *pager.BufferPool, rootPageID pager.PageID, length int) *DiskSecondaryBTree {
	return &DiskSecondaryBTree{
		pool:       pool,
		rootPageID: rootPageID,
		length:     length,
	}
}

func (t *DiskSecondaryBTree) RootPageID() pager.PageID { return t.rootPageID }
func (t *DiskSecondaryBTree) Len() int                 { return t.length }

// --- encode/decode ---

func decodeSecLeafPage(data []byte) secLeafPage {
	lp := secLeafPage{
		entryCount: binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]),
		nextLeaf:   binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]),
		prevLeaf:   binary.BigEndian.Uint32(data[leafOffPrevLeaf : leafOffPrevLeaf+4]),
	}
	pos := secLeafHdrSize
	lp.entries = make([]secLeafEntry, lp.entryCount)
	for i := 0; i < int(lp.entryCount); i++ {
		keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		key := make([]byte, keyLen)
		copy(key, data[pos:pos+keyLen])
		pos += keyLen
		lp.entries[i] = secLeafEntry{key: key}
	}
	return lp
}

func encodeSecLeafPage(lp secLeafPage, data []byte) {
	for i := range data {
		data[i] = 0
	}
	data[0] = flagLeaf
	binary.BigEndian.PutUint16(data[leafOffEntryCount:leafOffEntryCount+2], lp.entryCount)
	binary.BigEndian.PutUint32(data[leafOffNextLeaf:leafOffNextLeaf+4], lp.nextLeaf)
	binary.BigEndian.PutUint32(data[leafOffPrevLeaf:leafOffPrevLeaf+4], lp.prevLeaf)
	pos := secLeafHdrSize
	for _, e := range lp.entries {
		binary.BigEndian.PutUint16(data[pos:pos+2], uint16(len(e.key)))
		pos += 2
		copy(data[pos:], e.key)
		pos += len(e.key)
	}
}

func decodeSecInternalPage(data []byte) secInternalPage {
	ip := secInternalPage{
		entryCount: binary.BigEndian.Uint16(data[internalOffEntryCount : internalOffEntryCount+2]),
	}
	pos := internalHdrSize
	ip.keys = make([][]byte, ip.entryCount)
	ip.children = make([]pager.PageID, ip.entryCount+1)

	ip.children[0] = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	for i := 0; i < int(ip.entryCount); i++ {
		keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		key := make([]byte, keyLen)
		copy(key, data[pos:pos+keyLen])
		pos += keyLen
		ip.keys[i] = key
		ip.children[i+1] = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
	}
	return ip
}

func encodeSecInternalPage(ip secInternalPage, data []byte) {
	for i := range data {
		data[i] = 0
	}
	data[0] = 0 // not leaf
	binary.BigEndian.PutUint16(data[internalOffEntryCount:internalOffEntryCount+2], ip.entryCount)
	pos := internalHdrSize
	binary.BigEndian.PutUint32(data[pos:pos+4], ip.children[0])
	pos += 4
	for i := 0; i < int(ip.entryCount); i++ {
		binary.BigEndian.PutUint16(data[pos:pos+2], uint16(len(ip.keys[i])))
		pos += 2
		copy(data[pos:], ip.keys[i])
		pos += len(ip.keys[i])
		binary.BigEndian.PutUint32(data[pos:pos+4], ip.children[i+1])
		pos += 4
	}
}

func secLeafPageUsedBytes(entries []secLeafEntry) int {
	total := secLeafHdrSize
	for _, e := range entries {
		total += 2 + len(e.key)
	}
	return total
}

func searchSecLeaf(entries []secLeafEntry, key []byte) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(entries[mid].key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// --- Search ---

func (t *DiskSecondaryBTree) findLeaf(key []byte) (pager.PageID, error) {
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
		// inline internal page scan
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		pos := internalHdrSize
		child0 := pager.PageID(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		nextPageID := child0
		for i := 0; i < entryCount; i++ {
			keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			sepKey := data[pos : pos+keyLen]
			pos += keyLen
			childID := pager.PageID(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if bytes.Compare(sepKey, key) <= 0 {
				nextPageID = childID
			} else {
				break
			}
		}
		t.pool.UnpinPage(pageID, false)
		pageID = nextPageID
	}
}

func (t *DiskSecondaryBTree) findLeftmostLeaf() (pager.PageID, error) {
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
		next := pager.PageID(binary.BigEndian.Uint32(data[internalHdrSize : internalHdrSize+4]))
		t.pool.UnpinPage(pageID, false)
		pageID = next
	}
}

func (t *DiskSecondaryBTree) findSecRightmostLeaf() (pager.PageID, error) {
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
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		pos := internalHdrSize + 4 // skip child0
		for i := 0; i < entryCount; i++ {
			keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2 + keyLen + 4 // skip key + childID
		}
		// pos-4 is the last childID written
		lastChild := pager.PageID(binary.BigEndian.Uint32(data[pos-4 : pos]))
		t.pool.UnpinPage(pageID, false)
		pageID = lastChild
	}
}

// --- Insert ---

func (t *DiskSecondaryBTree) Insert(compositeKey []byte) {
	entry := secLeafEntry{key: compositeKey}

	leafID, err := t.findLeaf(compositeKey)
	if err != nil {
		panic(fmt.Sprintf("disk sec btree insert findLeaf: %v", err))
	}

	data, err := t.pool.FetchPage(leafID)
	if err != nil {
		panic(fmt.Sprintf("disk sec btree insert fetch: %v", err))
	}
	lp := decodeSecLeafPage(data)

	idx := searchSecLeaf(lp.entries, compositeKey)

	lp.entries = append(lp.entries, secLeafEntry{})
	copy(lp.entries[idx+1:], lp.entries[idx:])
	lp.entries[idx] = entry
	lp.entryCount++

	if secLeafPageUsedBytes(lp.entries) <= pager.PageSize {
		encodeSecLeafPage(lp, data)
		t.pool.UnpinPage(leafID, true)
		t.length++
		return
	}

	t.pool.UnpinPage(leafID, false)
	t.length++

	t.secInsertFromRoot(entry)
}

func (t *DiskSecondaryBTree) secInsertFromRoot(entry secLeafEntry) {
	splitKey, splitChild := t.secInsertRecursive(t.rootPageID, entry)
	if splitChild == pager.InvalidPageID {
		return
	}
	newRootID, newRootData, err := t.pool.NewPage()
	if err != nil {
		panic(fmt.Sprintf("disk sec btree: alloc new root: %v", err))
	}
	ip := secInternalPage{
		entryCount: 1,
		keys:       [][]byte{splitKey},
		children:   []pager.PageID{t.rootPageID, splitChild},
	}
	encodeSecInternalPage(ip, newRootData)
	t.pool.UnpinPage(newRootID, true)
	t.rootPageID = newRootID
}

func (t *DiskSecondaryBTree) secInsertRecursive(pageID pager.PageID, entry secLeafEntry) ([]byte, pager.PageID) {
	data, err := t.pool.FetchPage(pageID)
	if err != nil {
		panic(fmt.Sprintf("disk sec btree insert recursive: %v", err))
	}

	if isLeafPage(data) {
		lp := decodeSecLeafPage(data)
		idx := searchSecLeaf(lp.entries, entry.key)

		lp.entries = append(lp.entries, secLeafEntry{})
		copy(lp.entries[idx+1:], lp.entries[idx:])
		lp.entries[idx] = entry
		lp.entryCount++

		if secLeafPageUsedBytes(lp.entries) <= pager.PageSize {
			encodeSecLeafPage(lp, data)
			t.pool.UnpinPage(pageID, true)
			return nil, pager.InvalidPageID
		}

		mid := int(lp.entryCount) / 2
		newLeafID, newLeafData, err := t.pool.NewPage()
		if err != nil {
			panic(fmt.Sprintf("disk sec btree: alloc new leaf: %v", err))
		}

		rightEntries := make([]secLeafEntry, len(lp.entries[mid:]))
		copy(rightEntries, lp.entries[mid:])
		leftEntries := make([]secLeafEntry, mid)
		copy(leftEntries, lp.entries[:mid])

		oldNext := lp.nextLeaf
		newLP := secLeafPage{
			entryCount: uint16(len(rightEntries)),
			nextLeaf:   oldNext,
			prevLeaf:   pageID,
			entries:    rightEntries,
		}
		encodeSecLeafPage(newLP, newLeafData)
		t.pool.UnpinPage(newLeafID, true)

		if oldNext != pager.InvalidPageID {
			oldNextData, err := t.pool.FetchPage(oldNext)
			if err == nil {
				binary.BigEndian.PutUint32(oldNextData[7:11], newLeafID)
				t.pool.UnpinPage(oldNext, true)
			}
		}

		lp.entries = leftEntries
		lp.entryCount = uint16(mid)
		lp.nextLeaf = newLeafID
		encodeSecLeafPage(lp, data)
		t.pool.UnpinPage(pageID, true)

		splitKey := make([]byte, len(rightEntries[0].key))
		copy(splitKey, rightEntries[0].key)
		return splitKey, newLeafID
	}

	ip := decodeSecInternalPage(data)
	t.pool.UnpinPage(pageID, false)

	childIdx := int(ip.entryCount)
	lo, hi := 0, int(ip.entryCount)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(ip.keys[mid], entry.key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	childIdx = lo

	splitKey, splitChild := t.secInsertRecursive(ip.children[childIdx], entry)
	if splitChild == pager.InvalidPageID {
		return nil, pager.InvalidPageID
	}

	ip.keys = append(ip.keys, nil)
	copy(ip.keys[childIdx+1:], ip.keys[childIdx:])
	ip.keys[childIdx] = splitKey
	ip.children = append(ip.children, 0)
	copy(ip.children[childIdx+2:], ip.children[childIdx+1:])
	ip.children[childIdx+1] = splitChild
	ip.entryCount++

	if secInternalPageUsedBytes(ip) <= pager.PageSize {
		data2, err := t.pool.FetchPage(pageID)
		if err != nil {
			panic(fmt.Sprintf("disk sec btree: re-fetch internal: %v", err))
		}
		encodeSecInternalPage(ip, data2)
		t.pool.UnpinPage(pageID, true)
		return nil, pager.InvalidPageID
	}

	mid := int(ip.entryCount) / 2
	promoteKey := ip.keys[mid]

	newInternalID, newInternalData, err := t.pool.NewPage()
	if err != nil {
		panic(fmt.Sprintf("disk sec btree: alloc new internal: %v", err))
	}

	rightIP := secInternalPage{
		entryCount: uint16(int(ip.entryCount) - mid - 1),
		keys:       make([][]byte, int(ip.entryCount)-mid-1),
		children:   make([]pager.PageID, int(ip.entryCount)-mid),
	}
	copy(rightIP.keys, ip.keys[mid+1:])
	copy(rightIP.children, ip.children[mid+1:])
	encodeSecInternalPage(rightIP, newInternalData)
	t.pool.UnpinPage(newInternalID, true)

	leftIP := secInternalPage{
		entryCount: uint16(mid),
		keys:       ip.keys[:mid],
		children:   ip.children[:mid+1],
	}
	data2, err := t.pool.FetchPage(pageID)
	if err != nil {
		panic(fmt.Sprintf("disk sec btree: re-fetch for split: %v", err))
	}
	encodeSecInternalPage(leftIP, data2)
	t.pool.UnpinPage(pageID, true)

	return promoteKey, newInternalID
}

func secInternalPageUsedBytes(ip secInternalPage) int {
	total := internalHdrSize + 4
	for _, k := range ip.keys {
		total += 2 + len(k) + 4
	}
	return total
}

// --- Delete ---

func (t *DiskSecondaryBTree) Delete(compositeKey []byte) bool {
	deleted := t.secDeleteRecursive(t.rootPageID, compositeKey)
	if !deleted {
		return false
	}
	t.length--

	data, err := t.pool.FetchPage(t.rootPageID)
	if err != nil {
		return true
	}
	if !isLeafPage(data) {
		ip := decodeSecInternalPage(data)
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

func (t *DiskSecondaryBTree) secDeleteRecursive(pageID pager.PageID, key []byte) bool {
	data, err := t.pool.FetchPage(pageID)
	if err != nil {
		return false
	}

	if isLeafPage(data) {
		lp := decodeSecLeafPage(data)
		idx := searchSecLeaf(lp.entries, key)
		if idx >= int(lp.entryCount) || !bytes.Equal(lp.entries[idx].key, key) {
			t.pool.UnpinPage(pageID, false)
			return false
		}
		lp.entries = append(lp.entries[:idx], lp.entries[idx+1:]...)
		lp.entryCount--
		encodeSecLeafPage(lp, data)
		t.pool.UnpinPage(pageID, true)
		return true
	}

	ip := decodeSecInternalPage(data)
	t.pool.UnpinPage(pageID, false)

	childIdx := int(ip.entryCount)
	lo, hi := 0, int(ip.entryCount)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(ip.keys[mid], key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	childIdx = lo

	deleted := t.secDeleteRecursive(ip.children[childIdx], key)

	if deleted {
		t.secUpdateSeparatorKeys(pageID, key)
	}

	return deleted
}

func (t *DiskSecondaryBTree) secUpdateSeparatorKeys(pageID pager.PageID, deletedKey []byte) {
	data, err := t.pool.FetchPage(pageID)
	if err != nil {
		return
	}
	if isLeafPage(data) {
		t.pool.UnpinPage(pageID, false)
		return
	}
	ip := decodeSecInternalPage(data)
	changed := false
	for i := 0; i < int(ip.entryCount); i++ {
		if bytes.Equal(ip.keys[i], deletedKey) {
			minKey := t.secFindMinKey(ip.children[i+1])
			if minKey != nil {
				ip.keys[i] = minKey
				changed = true
			}
		}
	}
	if changed {
		encodeSecInternalPage(ip, data)
		t.pool.UnpinPage(pageID, true)
	} else {
		t.pool.UnpinPage(pageID, false)
	}
}

func (t *DiskSecondaryBTree) secFindMinKey(pageID pager.PageID) []byte {
	data, err := t.pool.FetchPage(pageID)
	if err != nil {
		return nil
	}
	if isLeafPage(data) {
		lp := decodeSecLeafPage(data)
		t.pool.UnpinPage(pageID, false)
		if lp.entryCount > 0 {
			result := make([]byte, len(lp.entries[0].key))
			copy(result, lp.entries[0].key)
			return result
		}
		return nil
	}
	ip := decodeSecInternalPage(data)
	t.pool.UnpinPage(pageID, false)
	return t.secFindMinKey(ip.children[0])
}

// --- Scan ---

// PrefixScan iterates entries whose key starts with prefix.
func (t *DiskSecondaryBTree) PrefixScan(prefix []byte, fn func(compositeKey []byte) bool) {
	if t.length == 0 {
		return
	}
	leafID, err := t.findLeaf(prefix)
	if err != nil {
		return
	}

	for pageID := leafID; pageID != pager.InvalidPageID; {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		nextLeaf := pager.PageID(binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]))
		pos := secLeafHdrSize
		done := false
		for i := 0; i < entryCount; i++ {
			keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			key := data[pos : pos+keyLen]
			pos += keyLen
			if !bytes.HasPrefix(key, prefix) {
				if bytes.Compare(key, prefix) > 0 {
					done = true
					break
				}
				continue
			}
			if !fn(key) {
				t.pool.UnpinPage(pageID, false)
				return
			}
		}
		t.pool.UnpinPage(pageID, false)
		if done {
			return
		}
		pageID = nextLeaf
	}
}

// RangeScan iterates entries in [from, to] range.
func (t *DiskSecondaryBTree) RangeScan(from, to []byte, fromInc, toInc bool, fn func(compositeKey []byte) bool) {
	if t.length == 0 {
		return
	}

	var startPageID pager.PageID
	if from == nil {
		var err error
		startPageID, err = t.findLeftmostLeaf()
		if err != nil {
			return
		}
	} else {
		var err error
		startPageID, err = t.findLeaf(from)
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
		pos := secLeafHdrSize
		done := false
		for i := 0; i < entryCount; i++ {
			keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			key := data[pos : pos+keyLen]
			pos += keyLen
			if from != nil {
				cmp := bytes.Compare(key, from)
				if fromInc {
					if cmp < 0 {
						continue
					}
				} else {
					if cmp <= 0 {
						continue
					}
				}
			}
			if to != nil {
				cmp := bytes.Compare(key, to)
				if toInc {
					if cmp > 0 {
						done = true
						break
					}
				} else {
					if cmp >= 0 {
						done = true
						break
					}
				}
			}
			if !fn(key) {
				t.pool.UnpinPage(pageID, false)
				return
			}
		}
		t.pool.UnpinPage(pageID, false)
		if done {
			return
		}
		pageID = nextLeaf
	}
}

// RangeScanReverse iterates entries in reverse order within [from, to] range.
func (t *DiskSecondaryBTree) RangeScanReverse(from, to []byte, fromInc, toInc bool, fn func(compositeKey []byte) bool) {
	if t.length == 0 {
		return
	}

	var startPageID pager.PageID
	if to == nil {
		var err error
		startPageID, err = t.findSecRightmostLeaf()
		if err != nil {
			return
		}
	} else {
		var err error
		startPageID, err = t.findLeaf(to)
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
		prevLeaf := pager.PageID(binary.BigEndian.Uint32(data[leafOffPrevLeaf : leafOffPrevLeaf+4]))

		// Collect entry offsets by forward scan, then iterate in reverse
		offsets := make([]int, entryCount)
		pos := secLeafHdrSize
		for i := 0; i < entryCount; i++ {
			offsets[i] = pos
			keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2 + keyLen
		}

		done := false
		for j := entryCount - 1; j >= 0; j-- {
			off := offsets[j]
			keyLen := int(binary.BigEndian.Uint16(data[off : off+2]))
			key := data[off+2 : off+2+keyLen]
			if to != nil {
				cmp := bytes.Compare(key, to)
				if toInc {
					if cmp > 0 {
						continue
					}
				} else {
					if cmp >= 0 {
						continue
					}
				}
			}
			if from != nil {
				cmp := bytes.Compare(key, from)
				if fromInc {
					if cmp < 0 {
						done = true
						break
					}
				} else {
					if cmp <= 0 {
						done = true
						break
					}
				}
			}
			if !fn(key) {
				t.pool.UnpinPage(pageID, false)
				return
			}
		}
		t.pool.UnpinPage(pageID, false)
		if done {
			return
		}
		pageID = prevLeaf
	}
}

// ForEach iterates all entries in key order.
func (t *DiskSecondaryBTree) ForEach(fn func(compositeKey []byte) bool) {
	if t.length == 0 {
		return
	}
	startPageID, err := t.findLeftmostLeaf()
	if err != nil {
		return
	}

	for pageID := startPageID; pageID != pager.InvalidPageID; {
		data, err := t.pool.FetchPage(pageID)
		if err != nil {
			return
		}
		entryCount := int(binary.BigEndian.Uint16(data[leafOffEntryCount : leafOffEntryCount+2]))
		nextLeaf := pager.PageID(binary.BigEndian.Uint32(data[leafOffNextLeaf : leafOffNextLeaf+4]))
		pos := secLeafHdrSize
		stopped := false
		for i := 0; i < entryCount; i++ {
			keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			key := data[pos : pos+keyLen]
			pos += keyLen
			if !fn(key) {
				stopped = true
				break
			}
		}
		t.pool.UnpinPage(pageID, false)
		if stopped {
			return
		}
		pageID = nextLeaf
	}
}
