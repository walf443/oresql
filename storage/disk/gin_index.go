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
// For bigram tokenizer, the search term is split into bigrams and the intersection
// of all posting lists is returned.
func (dgi *DiskGinIndex) MatchToken(token string) []int64 {
	if dgi.info.Tokenizer == "bigram" {
		return dgi.matchBigram(token)
	}
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

// matchBigram splits the search term into bigrams and returns the intersection
// of all posting lists. Bigrams are processed in ascending order of posting list
// size to minimize intermediate result sizes. Uses block-level skipping to avoid
// fully decoding posting lists whose block ranges don't overlap.
func (dgi *DiskGinIndex) matchBigram(token string) []int64 {
	bigrams := ginBigramTokenize(token)
	if len(bigrams) == 0 {
		return nil
	}

	// Get posting list sizes for all bigrams (read only the count varint)
	type bigramInfo struct {
		token string
		count uint64
	}
	infos := make([]bigramInfo, 0, len(bigrams))
	for _, bg := range bigrams {
		c := dgi.postingListCount(bg)
		if c == 0 {
			return nil // any empty posting list means no results
		}
		infos = append(infos, bigramInfo{bg, c})
	}

	// Sort by posting list size (smallest first)
	sort.Slice(infos, func(i, j int) bool { return infos[i].count < infos[j].count })

	// Start with the smallest posting list (decode fully)
	resultData := dgi.lookupRawPostingList(infos[0].token)
	if resultData == nil {
		return nil
	}

	// Intersect with remaining bigrams using block-level skipping
	for _, info := range infos[1:] {
		otherData := dgi.lookupRawPostingList(info.token)
		if otherData == nil {
			return nil
		}
		intersected := intersectBlockedPostingLists(resultData, otherData)
		if len(intersected) == 0 {
			return nil
		}
		// Re-encode for the next intersection round
		resultData = encodePostingList(intersected)
	}

	return decodePostingList(resultData)
}

// postingListCount returns the number of entries in the posting list for a token
// by reading only the count varint, without decoding the full posting list.
func (dgi *DiskGinIndex) postingListCount(token string) uint64 {
	prefix := ginEncodeToken(token)
	var count uint64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		postingData := compositeKey[len(prefix):]
		if len(postingData) > 0 {
			count, _ = readVarint(postingData, 0)
		}
		return false
	})
	return count
}

// lookupSingleToken returns the posting list for a single token.
func (dgi *DiskGinIndex) lookupSingleToken(token string) []int64 {
	prefix := ginEncodeToken(token)
	var keys []int64
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		postingData := compositeKey[len(prefix):]
		keys = decodePostingList(postingData)
		return false
	})
	return keys
}

// lookupRawPostingList returns the raw encoded posting list bytes for a single token.
// This avoids decoding when the caller needs to use block-level operations.
func (dgi *DiskGinIndex) lookupRawPostingList(token string) []byte {
	prefix := ginEncodeToken(token)
	var raw []byte
	dgi.tree.PrefixScan(prefix, func(compositeKey []byte) bool {
		postingData := compositeKey[len(prefix):]
		raw = make([]byte, len(postingData))
		copy(raw, postingData)
		return false
	})
	return raw
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
	for _, tok := range dgi.tokenizeText(text) {
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
	for _, tok := range dgi.tokenizeText(text) {
		dgi.removeFromPostingList(tok, key)
	}
}

// tokenizeText dispatches to the appropriate tokenizer based on the index configuration.
func (dgi *DiskGinIndex) tokenizeText(text string) []string {
	switch dgi.info.Tokenizer {
	case "bigram":
		return ginBigramTokenize(text)
	default:
		return ginTokenize(text)
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

// postingBlockSize is the number of keys per block in a blocked posting list.
const postingBlockSize = 128

// postingBlockHeader holds metadata for a single block in a blocked posting list.
type postingBlockHeader struct {
	base         int64 // first (minimum) key in the block
	last         int64 // last (maximum) key in the block
	count        int   // number of keys in this block
	dataByteSize int   // byte length of the delta data for this block
}

// blockedPostingList provides block-level access to a posting list.
// The headers are parsed eagerly (small), while block data is decoded on demand.
type blockedPostingList struct {
	totalCount int
	headers    []postingBlockHeader
	dataStart  int    // byte offset where block data begins
	data       []byte // the full encoded posting list bytes
}

// parseBlockedPostingList parses the headers of a blocked posting list without
// decoding the block data. This allows binary search on block bases.
func parseBlockedPostingList(data []byte) *blockedPostingList {
	if len(data) == 0 {
		return &blockedPostingList{}
	}
	totalCount, pos := readVarint(data, 0)
	if totalCount == 0 {
		return &blockedPostingList{}
	}
	numBlocks, pos := readVarint(data, pos)
	headers := make([]postingBlockHeader, numBlocks)
	for i := uint64(0); i < numBlocks; i++ {
		baseVal, pos2 := readVarint(data, pos)
		lastDelta, pos3 := readVarint(data, pos2)
		count, pos4 := readVarint(data, pos3)
		dataByteSize, pos5 := readVarint(data, pos4)
		headers[i] = postingBlockHeader{
			base:         int64(baseVal),
			last:         int64(baseVal) + int64(lastDelta),
			count:        int(count),
			dataByteSize: int(dataByteSize),
		}
		pos = pos5
	}
	return &blockedPostingList{
		totalCount: int(totalCount),
		headers:    headers,
		dataStart:  pos,
		data:       data,
	}
}

// decodeBlock decodes the keys in a single block by index.
func (bpl *blockedPostingList) decodeBlock(blockIdx int) []int64 {
	if blockIdx >= len(bpl.headers) {
		return nil
	}
	hdr := bpl.headers[blockIdx]
	if hdr.count == 0 {
		return nil
	}

	// Compute byte offset for this block's data
	offset := bpl.dataStart
	for i := 0; i < blockIdx; i++ {
		offset += bpl.headers[i].dataByteSize
	}

	keys := make([]int64, hdr.count)
	keys[0] = hdr.base
	pos := offset
	for i := 1; i < hdr.count; i++ {
		delta, newPos := readVarint(bpl.data, pos)
		pos = newPos
		keys[i] = keys[i-1] + int64(delta)
	}
	return keys
}

// encodePostingList encodes a sorted list of row keys using blocked delta + varint encoding.
//
// Format:
//
//	varint(totalCount) || varint(numBlocks) ||
//	[varint(base) || varint(lastDelta) || varint(count) || varint(dataByteSize)] * numBlocks ||
//	[varint(delta1) || varint(delta2) || ...] * numBlocks
//
// Each block contains up to postingBlockSize keys. The header stores the base
// (absolute first value), last-base delta, count, and byte size of the delta data,
// enabling binary search on block ranges and skipping to any block.
func encodePostingList(keys []int64) []byte {
	if len(keys) == 0 {
		return []byte{0}
	}

	numBlocks := (len(keys) + postingBlockSize - 1) / postingBlockSize

	// First pass: encode each block's delta data to get byte sizes
	blockData := make([][]byte, numBlocks)
	type blockMeta struct {
		base  int64
		last  int64
		count int
	}
	metas := make([]blockMeta, numBlocks)

	for b := 0; b < numBlocks; b++ {
		start := b * postingBlockSize
		end := start + postingBlockSize
		if end > len(keys) {
			end = len(keys)
		}
		blockKeys := keys[start:end]
		metas[b] = blockMeta{
			base:  blockKeys[0],
			last:  blockKeys[len(blockKeys)-1],
			count: len(blockKeys),
		}
		// Encode deltas (count-1 deltas, since base is stored in header)
		var bd []byte
		for i := 1; i < len(blockKeys); i++ {
			delta := uint64(blockKeys[i] - blockKeys[i-1])
			bd = appendVarint(bd, delta)
		}
		blockData[b] = bd
	}

	// Second pass: build the full encoded output
	buf := make([]byte, 0, len(keys)*5+numBlocks*20+10)
	buf = appendVarint(buf, uint64(len(keys)))
	buf = appendVarint(buf, uint64(numBlocks))

	// Block headers
	for b := 0; b < numBlocks; b++ {
		buf = appendVarint(buf, uint64(metas[b].base))
		buf = appendVarint(buf, uint64(metas[b].last-metas[b].base))
		buf = appendVarint(buf, uint64(metas[b].count))
		buf = appendVarint(buf, uint64(len(blockData[b])))
	}

	// Block data
	for b := 0; b < numBlocks; b++ {
		buf = append(buf, blockData[b]...)
	}

	return buf
}

// decodePostingList decodes a blocked delta + varint encoded posting list.
func decodePostingList(data []byte) []int64 {
	bpl := parseBlockedPostingList(data)
	if bpl.totalCount == 0 {
		return nil
	}
	keys := make([]int64, 0, bpl.totalCount)
	for i := range bpl.headers {
		keys = append(keys, bpl.decodeBlock(i)...)
	}
	return keys
}

// intersectBlockedPostingLists intersects two encoded posting lists using
// block-level skipping. Blocks whose ranges don't overlap are skipped entirely.
func intersectBlockedPostingLists(dataA, dataB []byte) []int64 {
	a := parseBlockedPostingList(dataA)
	b := parseBlockedPostingList(dataB)
	if a.totalCount == 0 || b.totalCount == 0 {
		return nil
	}

	var result []int64
	bi := 0 // current block index in b

	for ai := 0; ai < len(a.headers); ai++ {
		aHdr := a.headers[ai]

		// Advance b until we find a block that could overlap with a's current block
		for bi < len(b.headers) && b.headers[bi].last < aHdr.base {
			bi++
		}
		if bi >= len(b.headers) {
			break
		}

		// Skip a's block if it ends before b's current block starts
		if aHdr.last < b.headers[bi].base {
			continue
		}

		// Blocks potentially overlap — decode a's block
		aKeys := a.decodeBlock(ai)

		// Check against all b blocks that could overlap with a's range
		for bj := bi; bj < len(b.headers); bj++ {
			bHdr := b.headers[bj]
			if bHdr.base > aHdr.last {
				break // no more b blocks can overlap
			}
			bKeys := b.decodeBlock(bj)

			// Merge-intersect two sorted slices
			i, j := 0, 0
			for i < len(aKeys) && j < len(bKeys) {
				if aKeys[i] == bKeys[j] {
					result = append(result, aKeys[i])
					i++
					j++
				} else if aKeys[i] < bKeys[j] {
					i++
				} else {
					j++
				}
			}
		}
	}
	return result
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

// ginBigramTokenize splits text into 2-character overlapping tokens (bigrams).
func ginBigramTokenize(text string) []string {
	lower := strings.ToLower(text)
	runes := []rune(lower)
	if len(runes) < 2 {
		if len(runes) == 1 {
			return []string{string(runes)}
		}
		return nil
	}
	seen := make(map[string]struct{})
	tokens := make([]string, 0, len(runes)-1)
	for i := 0; i < len(runes)-1; i++ {
		bigram := string(runes[i : i+2])
		if _, ok := seen[bigram]; !ok {
			seen[bigram] = struct{}{}
			tokens = append(tokens, bigram)
		}
	}
	return tokens
}
