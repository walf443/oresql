package disk

import (
	"sort"
	"strings"
	"unicode"

	"github.com/walf443/oresql/storage"
)

// Compile-time verification that DiskGinIndex satisfies GinIndexReader.
var _ storage.GinIndexReader = (*DiskGinIndex)(nil)

// DiskGinIndex implements storage.GinIndexReader backed by a DiskSecondaryBTree.
//
// Each entry in the tree stores a posting list:
//
//	key = EncodeValue(token) || postingListBytes
//
// where postingListBytes is a delta + varint encoded sorted list of row keys.
// This achieves one BTree entry per unique token instead of one per (token, rowKey) pair.
type DiskGinIndex struct {
	info *storage.IndexInfo
	tree *DiskSecondaryBTree
}

func (dgi *DiskGinIndex) GetInfo() *storage.IndexInfo {
	return dgi.info
}

// MatchToken returns sorted row keys whose indexed column contains the given token.
func (dgi *DiskGinIndex) MatchToken(token string) []int64 {
	lower := strings.ToLower(token)
	prefix := ginEncodeToken(lower)

	var keys []int64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		postingData := compositeKey[len(prefix):]
		keys = decodePostingList(postingData)
		return false // only one entry per token
	})
	return keys
}

// MatchPrefix returns sorted row keys whose indexed column contains a token
// that starts with the given prefix. Uses PrefixScan for efficient lookup.
func (dgi *DiskGinIndex) MatchPrefix(prefix string) []int64 {
	lower := strings.ToLower(prefix)
	// TEXT encoding uses 0x03 + raw bytes; we scan by the prefix bytes without
	// the null terminator so that all tokens starting with `lower` are matched.
	scanPrefix := append([]byte{0x03}, []byte(lower)...)

	keyMap := make(map[int64]struct{})
	dgi.tree.PrefixScan(scanPrefix, func(compositeKey []byte) bool {
		// Extract the full token prefix (including null terminator) to find posting list data
		tokenEnd := -1
		for i := 1; i < len(compositeKey); i++ {
			if compositeKey[i] == 0x00 {
				tokenEnd = i
				break
			}
		}
		if tokenEnd < 0 {
			return true
		}
		postingData := compositeKey[tokenEnd+1:]
		for _, k := range decodePostingList(postingData) {
			keyMap[k] = struct{}{}
		}
		return true // continue scanning
	})
	keys := make([]int64, 0, len(keyMap))
	for k := range keyMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// AddRow indexes all tokens from the TEXT value in the specified column.
func (dgi *DiskGinIndex) AddRow(key int64, row storage.Row) {
	colIdx := dgi.info.ColumnIdxs[0]
	val := row[colIdx]
	text, ok := val.(string)
	if !ok {
		return // NULL or non-string values are not indexed
	}
	for _, tok := range ginTokenize(text) {
		dgi.addToPostingList(tok, key)
	}
}

// RemoveRow removes all token entries for the given row.
func (dgi *DiskGinIndex) RemoveRow(key int64, row storage.Row) {
	colIdx := dgi.info.ColumnIdxs[0]
	val := row[colIdx]
	text, ok := val.(string)
	if !ok {
		return
	}
	for _, tok := range ginTokenize(text) {
		dgi.removeFromPostingList(tok, key)
	}
}

// addToPostingList adds a rowKey to the posting list for the given token.
func (dgi *DiskGinIndex) addToPostingList(token string, rowKey int64) {
	prefix := ginEncodeToken(token)

	// Find existing posting list
	var oldEntry []byte
	var existing []int64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		oldEntry = make([]byte, len(compositeKey))
		copy(oldEntry, compositeKey)
		existing = decodePostingList(compositeKey[len(prefix):])
		return false
	})

	// Insert rowKey in sorted position
	pos := sort.Search(len(existing), func(i int) bool { return existing[i] >= rowKey })
	if pos < len(existing) && existing[pos] == rowKey {
		return // already exists
	}
	existing = append(existing, 0)
	copy(existing[pos+1:], existing[pos:])
	existing[pos] = rowKey

	// Replace entry
	if oldEntry != nil {
		dgi.tree.Delete(oldEntry)
	}
	newEntry := append(prefix, encodePostingList(existing)...)
	dgi.tree.Insert(newEntry)
}

// removeFromPostingList removes a rowKey from the posting list for the given token.
func (dgi *DiskGinIndex) removeFromPostingList(token string, rowKey int64) {
	prefix := ginEncodeToken(token)

	var oldEntry []byte
	var existing []int64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		oldEntry = make([]byte, len(compositeKey))
		copy(oldEntry, compositeKey)
		existing = decodePostingList(compositeKey[len(prefix):])
		return false
	})

	if oldEntry == nil {
		return
	}

	// Remove rowKey
	pos := sortSearchInt64s(existing, rowKey)
	if pos >= len(existing) || existing[pos] != rowKey {
		return // not found
	}
	existing = append(existing[:pos], existing[pos+1:]...)

	dgi.tree.Delete(oldEntry)
	if len(existing) > 0 {
		newEntry := append(prefix, encodePostingList(existing)...)
		dgi.tree.Insert(newEntry)
	}
}

// encodePostingList encodes a sorted list of row keys using delta + varint encoding.
//
// Format: varint(count) || varint(keys[0]) || varint(keys[1]-keys[0]) || ...
func encodePostingList(keys []int64) []byte {
	if len(keys) == 0 {
		return []byte{0}
	}
	// Pre-allocate: worst case ~10 bytes per varint
	buf := make([]byte, 0, len(keys)*5+10)
	buf = appendVarint(buf, uint64(len(keys)))
	buf = appendVarint(buf, uint64(keys[0]))
	for i := 1; i < len(keys); i++ {
		delta := uint64(keys[i] - keys[i-1])
		buf = appendVarint(buf, delta)
	}
	return buf
}

// decodePostingList decodes a delta + varint encoded posting list.
func decodePostingList(data []byte) []int64 {
	if len(data) == 0 {
		return nil
	}
	count, pos := readVarint(data, 0)
	if count == 0 {
		return nil
	}
	keys := make([]int64, count)
	val, pos := readVarint(data, pos)
	keys[0] = int64(val)
	for i := uint64(1); i < count; i++ {
		delta, newPos := readVarint(data, pos)
		pos = newPos
		keys[i] = keys[i-1] + int64(delta)
	}
	return keys
}

// appendVarint appends a varint-encoded uint64 to buf.
func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// readVarint reads a varint from data at the given position.
func readVarint(data []byte, pos int) (uint64, int) {
	var v uint64
	var shift uint
	for pos < len(data) {
		b := data[pos]
		pos++
		v |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return v, pos
		}
		shift += 7
	}
	return v, pos
}

// sortSearchInt64s is not in stdlib, so we provide it here.
func sortSearchInt64s(a []int64, x int64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

// ginEncodeToken encodes a token as a sort-preserving key prefix.
// Uses TEXT encoding: 0x03 + raw bytes + 0x00 (null terminator).
func ginEncodeToken(token string) []byte {
	return storage.EncodeValueBytes(nil, token)
}

// ginTokenize splits text into lowercase tokens by whitespace and punctuation.
func ginTokenize(text string) []string {
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	tokens := make([]string, len(words))
	for i, w := range words {
		tokens[i] = strings.ToLower(w)
	}
	return tokens
}
